import ast
import datetime
import json
import time

from cloudaux.aws.sts import boto3_cached_conn
from marshmallow import Schema, fields, post_load

from repokid import CONFIG as CONFIG
import repokid.cli.repokid_cli as cli
import repokid.utils.dynamo as dynamo


class Message(object):
    def __init__(self, command, account, role_name, respond_channel, respond_user=None, requestor=None, reason=None,
                 selection=None):
        self.command = command
        self.account = account
        self.role_name = role_name
        self.respond_channel = respond_channel
        self.respond_user = respond_user
        self.requestor = requestor
        self.reason = reason
        self.selection = selection


class MessageSchema(Schema):
    command = fields.Str(required=True)
    account = fields.Str(required=True)
    role_name = fields.Str(required=True)
    respond_channel = fields.Str(required=True)
    respond_user = fields.Str()
    requestor = fields.Str()
    reason = fields.Str()
    selection = fields.Str()

    @post_load
    def make_message(self, data):
        return Message(**data)


def init_messaging(**config):
    sqs = boto3_cached_conn('sqs', service_type='client', assume_role=config.get('assume_role', None),
                            session_name=config['session_name'], region=config['region'])

    sns = boto3_cached_conn('sns', service_type='client', assume_role=config.get('assume_role', None),
                            session_name=config['session_name'], region=config['region'])
    return sqs, sns


def list_repoable_services(dynamo_table, message, config=None):
    role_id = dynamo.find_role_in_cache(dynamo_table, message.account, message.role_name)

    if not role_id:
        return (False, 'Unable to find role {} in account {}'.format(message.role_name, message.account))
    else:
        role_data = dynamo.get_role_data(dynamo_table, role_id, fields=['RepoableServices'])
        repoable_services = role_data['RepoableServices']
        return (True, 'Repoable services from role {} in account {}: {}'.format(message.role_name, message.account,
                                                                                repoable_services))


def list_role_rollbacks(dynamo_table, message, config=None):
    role_id = dynamo.find_role_in_cache(dynamo_table, message.account, message.role_name)

    if not role_id:
        return (False, 'Unable to find role {} in account {}'.format(message.role_name, message.account))
    else:
        role_data = dynamo.get_role_data(dynamo_table, role_id, fields=['Policies'])
        return_val = 'Restorable versions for role {} in account {}\n'.format(message.role_name, message.account)
        for index, policy_version in enumerate(role_data['Policies']):
            return_val += '({:>3}):  {:<5}     {:<15}  {}\n'.format(index, len(str(policy_version['Policy'])),
                                                                    policy_version['Discovered'],
                                                                    policy_version['Source'])
        return (True, return_val)


def opt_out(dynamo_table, message, config=None):
    if not message.reason or not message.requestor:
        return (False, 'Reason and requestor must be specified')

    role_id = dynamo.find_role_in_cache(dynamo_table, message.account, message.role_name)

    if not role_id:
        return (False, 'Unable to find role {} in account {}'.format(message.role_name, message.account))

    role_data = dynamo.get_role_data(dynamo_table, role_id, fields=['OptOut'])
    if 'OptOut' in role_data and role_data['OptOut']:

        return (False, 'Role {} in account {} is already opted out by {} for reason {} until {}'.format(
            message.role_name, message.account, role_data['OptOut']['owner'], role_data['OptOut']['reason'],
            time.strftime('%m/%d/%y', time.localtime(role_data['OptOut']['expire']))))

    else:
        current_dt = datetime.datetime.fromtimestamp(time.time())
        expire_dt = current_dt + datetime.timedelta(config['opt_out_period_days'])
        expire_epoch = int((expire_dt - datetime.datetime(1970, 1, 1)).total_seconds())
        new_opt_out = {'owner': message.requestor, 'reason': message.reason, 'expire': expire_epoch}
        dynamo.set_role_data(dynamo_table, role_id, {'OptOut': new_opt_out})
        return (True, 'Role {} in account {} opted-out until {}'.format(message.role_name, message.account,
                                                                        expire_dt.strftime('%m/%d/%y')))


def remove_opt_out(dynamo_table, message, config=None):
    role_id = dynamo.find_role_in_cache(dynamo_table, message.account, message.role_name)

    if not role_id:
        return (False, 'Unable to find role {} in account {}'.format(message.role_name, message.account))

    role_data = dynamo.get_role_data(dynamo_table, role_id, fields=['OptOut'])

    if 'OptOut' not in role_data or not role_data['OptOut']:
        return (False, 'Role {} in account {} wasn\'t opted out'.format(message.role_name, message.account))
    else:
        dynamo.set_role_data(dynamo_table, role_id, {'OptOut': {}})
        return (True, 'Cancelled opt-out for role {} in account {}'.format(message.role_name, message.account))


def rollback_role(dynamo_table, message, config=None):
    if not message.selection:
        return (False, 'Rollback must contain a selection number')

    errors = cli.rollback_role(message.account, message.role_name, dynamo_table, CONFIG, selection=message.selection,
                               commit=True)
    if errors:
        return (False, 'Errors during rollback: {}'.foramt(errors))
    else:
        return (True, 'Successfully rolled back role {} in account {}'.format(message.role_name, message.account))


def send_message(sns_object, topic_arn, message_dict):
    sns_object.publish(TopicArn=topic_arn, Message=json.dumps(message_dict))


RESPONDER_FUNCTIONS = dict()
RESPONDER_FUNCTIONS['list_repoable_services'] = list_repoable_services
RESPONDER_FUNCTIONS['list_role_rollbacks'] = list_role_rollbacks
RESPONDER_FUNCTIONS['opt_out'] = opt_out
RESPONDER_FUNCTIONS['remove_opt_out'] = remove_opt_out
RESPONDER_FUNCTIONS['rollback_role'] = rollback_role


def main():
    (to_repokid_sqs, from_repokid_sns) = init_messaging(**CONFIG['reactor'])
    dynamo_table = dynamo.dynamo_get_or_create_table(**CONFIG['dynamo_db'])
    message_schema = MessageSchema()

    while 1:
        message = to_repokid_sqs.receive_message(QueueUrl=CONFIG['reactor']['to_rr_queue'],
                                                 MaxNumberOfMessages=1,
                                                 WaitTimeSeconds=20)

        if 'Messages' in message:
            message_dict = {}
            successful = False
            result_message = ''
            receipt_handle = None

            try:
                receipt_handle = message['Messages'][0]['ReceiptHandle']
                message_dict = ast.literal_eval(message['Messages'][0]['Body'])

            except KeyError:
                result_message = 'Received malformed SQS message'

            if message_dict:
                result = message_schema.load(message_dict)

                if result.errors:
                    result_message = 'Malformed message: {}'.format(result['errors'])
                else:
                    command_data = result.data
                    try:
                        (successful, result_message) = RESPONDER_FUNCTIONS[command_data.command](
                            dynamo_table, command_data, config=CONFIG)

                    except KeyError:
                        result_message = 'Unknown function {}'.format(command_data.command)

            respond_message = {'message': '@{} {}'.format(command_data.respond_user, result_message),
                               'channel': command_data.respond_channel,
                               'title': 'Repokid Success' if successful else 'Repokid Failure'}

            send_message(from_repokid_sns, CONFIG['reactor']['from_rr_sns'], respond_message)

            if receipt_handle:
                to_repokid_sqs.delete_message(QueueUrl=CONFIG['reactor']['to_rr_queue'],
                                              ReceiptHandle=receipt_handle)


if __name__ == "__main__":
    main()
