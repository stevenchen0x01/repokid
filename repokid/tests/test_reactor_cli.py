import copy
import datetime
from mock import call, patch

import repokid.cli.reactor_cli as reactor_cli


DYNAMO_TABLE = None
MESSAGE = reactor_cli.Message('command', 'account', 'role', 'respond_channel', respond_user='some_user',
                              requestor='a_requestor', reason='some_reason', selection='some_selection')


class TestReactorCLI(object):
    def test_message_creation(self):
        test_message = MESSAGE
        assert test_message.command == 'command'
        assert test_message.account == 'account'
        assert test_message.role_name == 'role'
        assert test_message.respond_channel == 'respond_channel'
        assert test_message.respond_user == 'some_user'
        assert test_message.requestor == 'a_requestor'
        assert test_message.reason == 'some_reason'
        assert test_message.selection == 'some_selection'

    def test_schema(self):
        schema = reactor_cli.MessageSchema()

        # happy path
        test_message = {'command': 'list_repoable_services', 'account': '123', 'role_name': 'abc',
                        'respond_channel': 'channel', 'respond_user': 'user'}
        result = schema.load(test_message)
        assert not result.errors

        # missing required field command
        test_message = {'account': '123', 'role_name': 'abc', 'respond_channel': 'channel', 'respond_user': 'user'}
        result = schema.load(test_message)
        assert result.errors

    @patch('repokid.utils.dynamo.find_role_in_cache')
    @patch('repokid.utils.dynamo.get_role_data')
    def test_list_repoable_services(self, mock_get_role_data, mock_find_role_in_cache):
        mock_find_role_in_cache.side_effect = [None, 'ROLE_ID_A']

        (success, _) = reactor_cli.list_repoable_services(DYNAMO_TABLE, MESSAGE)
        assert not success

        (success, _) = reactor_cli.list_repoable_services(DYNAMO_TABLE, MESSAGE)
        assert success

    @patch('repokid.utils.dynamo.find_role_in_cache')
    @patch('repokid.utils.dynamo.get_role_data')
    def test_list_role_rollbacks(self, mock_get_role_data, mock_find_role_in_cache):
        mock_find_role_in_cache.side_effect = [None, 'ROLE_ID_A']

        (success, _) = reactor_cli.list_role_rollbacks(DYNAMO_TABLE, MESSAGE)
        assert not success

        (success, _) = reactor_cli.list_repoable_services(DYNAMO_TABLE, MESSAGE)
        assert success

    @patch('time.time')
    @patch('repokid.utils.dynamo.find_role_in_cache')
    @patch('repokid.utils.dynamo.get_role_data')
    @patch('repokid.utils.dynamo.set_role_data')
    def test_opt_out(self, mock_set_role_data, mock_get_role_data, mock_find_role_in_cache, mock_time):
        mock_find_role_in_cache.side_effect = [None, 'ROLE_ID_A']
        GET_ROLE_DATA_FOR_FIND = {}
        mock_get_role_data.side_effect = [GET_ROLE_DATA_FOR_FIND,  # role not found
                                          GET_ROLE_DATA_FOR_FIND,  # opt out exists
                                          {'OptOut': {'owner': 'somebody', 'reason': 'because'}},
                                          GET_ROLE_DATA_FOR_FIND,
                                          {'OptOut': {}}  # success
                                          ]

        config = {'opt_out_period_days': 90}

        mock_time.return_value = 0

        current_dt = datetime.datetime.fromtimestamp(0)
        expire_dt = current_dt + datetime.timedelta(config['opt_out_period_days'])
        expire_epoch = int((expire_dt - datetime.datetime(1970, 1, 1)).total_seconds())

        bad_message = copy.copy(MESSAGE)
        bad_message.reason = None
        # message missing reason
        (success, _) = reactor_cli.opt_out(DYNAMO_TABLE, bad_message, config)
        assert not success

        # role not found
        (success, _) = reactor_cli.opt_out(DYNAMO_TABLE, MESSAGE, config)
        assert not success

        (success, msg) = reactor_cli.opt_out(DYNAMO_TABLE, MESSAGE, config)
        assert success
        assert mock_set_role_data.mock_calls == [call(DYNAMO_TABLE, 'ROLE_ID_A',
                                                      {'OptOut': {'owner': MESSAGE.requestor,
                                                                  'reason': MESSAGE.reason,
                                                                  'expire': expire_epoch}})]
