import settings
from sh_util.retry import retry
import kazoo.exceptions as exceptions
import logging
from requests.exceptions import RequestException

DEFAULT_RING_TIMEOUT = 30
DEFAULT_KAZOO_CALL_FLOW = {
    'numbers':[],
    'flow':{
        'module':'ring_group',
        'data':{
            'name':'',
            'endpoints':[],
            'timeout': DEFAULT_RING_TIMEOUT,
            "strategy":"simultaneous",
        },
        'children':{
            '_':{
                'module':'voicemail',
                'data':{
                    'id':''
                },
                'children':{}
            }
        }
    }
}

NO_MATCH_CALL_FLOW = {
   "featurecode": {
   },
   "numbers": [
       "no_match"
   ],
   "flow": {
       "children": {
       },
       "data": {
       },
       "module": "offnet"
   }
}



class KazooClient(object):

    kazooCli = settings.KAZOO_CLI
    authToken = None

    @retry(2)
    def authenticate(self):

        try:

            self.authToken = self.kazooCli.authenticate()

        except (exceptions.KazooApiError, exceptions.InvalidConfigurationError,
                exceptions.KazooApiBadDataError, TypeError, RequestException) as e:
            self.authToken = None
            logging.error(u'Error authenticating against kazoo: '.format(e))
            import traceback
            traceback.print_exc(e)
            return False

        return True

    def getEnterpriseAccount(self, id):
        pass

    def createEnterpriseAccount(self, enterpriseId, name):
        '''
        Given a enterprise id and name, create an account on Kazoo

        enterpriseId Id of the EnterpriseAccount to be created on kazoo (unique)
        name Name of the EnterpriseAccount to be created on kazoo
        '''


        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:

            if enterpriseId is None or name is None:
                raise exceptions.KazooApiError(u'EnterpriseId () and Name () must be provided'.format(enterpriseId, name))

            result = self.kazooCli.create_account(
                {
                    u'name':str(enterpriseId),
                    u'enterprise_id':str(enterpriseId),
                    u'enterprise_name':name,
                    u'realm':u'{}.sip.sendhub.com'.format(enterpriseId)
                }
            )

            # create the no-match call flow for this account
            # so the global carrier stuff works
            self.kazooCli.create_callflow(result['data']['id'], NO_MATCH_CALL_FLOW)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return result

    def getUser(self, accountId, kazooUserId):
        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:

            if accountId is None or kazooUserId is None:
                raise exceptions.KazooApiError(u'accountId {} and kazooUserId {} must be provided'.format(accountId, kazooUserId))

            result = self.kazooCli.get_user(accountId, kazooUserId)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return result

    def _softPhoneTemplate(self, userId, enterpriseId, ownerId, number, username, password):
        return {
            u'name':number,
            u'sip': {
                u'realm':u'{}.sip.sendhub.com'.format(enterpriseId if enterpriseId is not None else 'default'),
                u'method':u'password',
                u'username':username,
                u'password':password,
            },
            u"caller_id": {
                u"external": {
                    u"number": number,
                    u"name": u""
                },
                u"internal": {
                    u"name": u"",
                    u"number": number
                }
            },
            u'caller_id':{
                u'external':{u'number':number}
            },
            u'device_type':u'softphone',
            u'owner_id':str(ownerId),
            u'sendhub_number':number
        }

    def _physicalPhoneTemplate(self, userId, enterpriseId, ownerId, number, type=u'cellphone'):
        return {
            u'name': number,
            u'caller_id':{
                u'external':{u'number':number}
            },
            u'device_type': type,
            u'call_forward': {
                u'enabled': True,
                u'substitute': True,
                u'require_keypress': True,
                u'keep_caller_id': True,
                u'direct_calls_only': False,
                u'ignore_early_media': True,
                u'number': number
            },
            u'owner_id':str(ownerId),
            u'forwarding_number':number
        }

    def createDevice(self, type, accountId, userId, enterpriseId, ownerId, number, username=u'', password=u''):
        assert type in (u'softphone', u'cellphone', u'landline')

        logging.info('createDevice invoked with {},{},{},{},{},{},{}'.format(type, accountId, userId, enterpriseId, ownerId, number, username))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if not authenticated:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        if type == u'softphone':
            deviceParams = self._softPhoneTemplate(userId, enterpriseId, ownerId, number, username, password)
        else:
            deviceParams = self._physicalPhoneTemplate(userId, enterpriseId, ownerId, number)

        try:
            return self.kazooCli.create_device(accountId, deviceParams)
        except exceptions.KazooApiBadDataError as e:
            if ('sip.username' in e.field_errors and 'unique' in e.field_errors['sip.username']) is False:
                logging.error('Unexpected error creating device: {}'.format(e))
                raise
            logging.info('SIP Device already exists for username: {}'.format(username))

        return None

    def createUser(self, accountId, name, userId, password, enterpriseId, sipUsername, sipPassword, softPhoneNumber=None, cellPhoneNumbers=[], email=None):
        '''
        Create a user on Kazoo within an given enterprise or within the general sendhub enterprise

        accountId: Account on kazoo which this user will be created under
        name: ShUser name
        userId: Id of the user
        password: Password to set on kazoo
        enterpriseId: The id of the enterprise account. The account must already exist on kazoo.
        sipUsername: SIP device username for the web device
        sipPassword: SIP password for the web device
        softPhoneNumber: Voip number too add for this account
        cellPhoneNumbers: Cell phone numbers to add for this account
        email: Email address for this account (will be set to a unique-bogus email if not specified as kazoo requires it)
        '''

        logging.info('createUser invoked with {},{},{},{},{},{},{},{}'.format(accountId, name, userId, password, enterpriseId, sipUsername, softPhoneNumber, cellPhoneNumbers))


        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        shortNumber = None
        userDetails = {
            'id':None,
            'first_name':None,
            'username':None,
            'voicemailId':None,
            'softphoneId':None,
            'cellphoneIds':[],
            'callFlowId':None,
            'autoAttendantMenuId':None
        }

        if authenticated:

            if name is None or userId is None or password is None:
                raise exceptions.KazooApiError(u'userId () and Name () must be provided'.format(userId, name))

            createUserResult = None
            try:
                createUserResult = self.kazooCli.create_user(
                    accountId,
                    {
                        u'first_name':name,
                        u'last_name':'SH',
                        u'username':str(userId),
                        u'password':password,
                        u'enterprise_id':str(enterpriseId),
                        u'email':u'{}@no-reply.sendhub.com'.format(email) if email is None else email
                    }
                )

                if createUserResult['status'] == 'success':

                    callFlow = DEFAULT_KAZOO_CALL_FLOW

                    callFlow['numbers'].append(str(userId))
                    callFlow['flow']['data']['endpoints'].append(
                        {
                            "endpoint_type":"user",
                            "id":str(createUserResult['data']['id']),
                            "delay":"0",
                            "timeout": '{}'.format(DEFAULT_RING_TIMEOUT)
                        }
                    )

                    softPhoneDeviceResult = None
                    if softPhoneNumber is not None:
                        shortNumber = softPhoneNumber[2:] if softPhoneNumber.startswith("+1") else softPhoneNumber
                        self.kazooCli.create_phone_number(accountId, shortNumber)
                        # arbitrary data cannot be passed in the create request so we gotta send two
                        self.kazooCli.update_phone_number(accountId, shortNumber, {'userId':str(userId)})

                        callFlow['numbers'].append(softPhoneNumber)

                        softPhoneDeviceResult = self.createDevice(type=u'softphone', accountId=accountId, userId=userId, enterpriseId=enterpriseId,
                                          ownerId=createUserResult['data']['id'], number=shortNumber, username=sipUsername, password=sipPassword)

                    cellPhoneResults = []
                    for number in cellPhoneNumbers:
                        if number is not None:
                            shortNumber = number[2:] if number.startswith("+1") else number
                            cellPhoneResults.append(self.createDevice(type=u'cellphone', accountId=accountId, userId=userId, enterpriseId=enterpriseId,
                                          ownerId=createUserResult['data']['id'], number=shortNumber))


                    # the following requires that the schema be changed on kazoo.
                    # so if this fails, then check
                    vmBoxObj = self.kazooCli.create_voicemail_box(
                        accountId,
                        {
                            'mailbox':str(userId),
                            'require_pin':False,
                            'name':str(userId),
                            'check_if_owner': True,
                            'owner_id':str(createUserResult['data']['id'])
                        }
                    )
                    callFlow['flow']['children']['_']['data']['id'] = vmBoxObj['data']['id']

                    callFlowResult = self.kazooCli.create_callflow(accountId, callFlow)

                    autoAttendantMenuResult = self.kazooCli.create_menu(
                        accountId,
                        {
                            'name':str(userId),
                            'retries' : 3,
                            'timeout' : '10000',
                            'max_extension_length':'1'
                        }
                    )

                    userDetails['id'] = createUserResult['data']['id']
                    userDetails['name'] = createUserResult['data']['first_name']
                    userDetails['username'] = createUserResult['data']['username']
                    userDetails['enterpriseId'] = createUserResult['data']['enterprise_id']
                    userDetails['voicemailId'] = vmBoxObj['data']['id']
                    userDetails['softphoneId'] = softPhoneDeviceResult['data']['id']
                    userDetails['cellphoneIds'] = [{'id':cellPhoneResult['data']['id'], 'number':cellPhoneResult['data']['call_forward']['number']} for cellPhoneResult in cellPhoneResults]
                    userDetails['callFlowId'] = callFlowResult['data']['id']
                    userDetails['autoAttendantMenuId'] = autoAttendantMenuResult['data']['id']

            except Exception as e:

                logging.error(u'Unable to create user on Kazoo: {}'.format(e))
                import traceback
                traceback.print_exc(e)

                # if we couldn't create the user then try to delete them so
                # we can try again
                if createUserResult is not None and createUserResult['status'] == 'success':
                    logging.error(u'Deleting partially created user')
                    self.deleteUser(accountId, createUserResult['data']['id'], shortNumber, userDetails['cellphoneIds'].extend([userDetails['softphoneId']]),
                                    userDetails['voicemailId'], userDetails['callFlowId'], userDetails['autoAttendantMenuId'])
                raise

        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return userDetails

    def deleteAccount(self, accountId):
        logging.info('Deleting account {} on Kazoo'.format(accountId))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            try:
                self.kazooCli.delete_account(accountId)
            except Exception as e:
                logging.error('Unable to delete account: {}'.format(accountId))
                logging.error(e)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def deleteUser(self, accountId, userId, phoneNumber=None, deviceIds=[], voicemailId=None, callFlowId=None, menuId=None):
        logging.info('Deleting user on Kazoo with account {} and user {}'.format(accountId, userId))


        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:

            if menuId is not None:
                try:
                    self.kazooCli.delete_menu(accountId, menuId)
                except Exception as e:
                    logging.error('Unable to delete menu: {}'.format(menuId))
                    logging.error(e)

            if callFlowId is not None:
                try:
                    self.kazooCli.delete_callflow(accountId, callFlowId)
                except Exception as e:
                    logging.error('Unable to delete callflow: {}'.format(callFlowId))
                    logging.error(e)

            if voicemailId is not None:
                try:
                    self.kazooCli.delete_voicemail_box(accountId, voicemailId)
                except Exception as e:
                    logging.error('Unable to delete vm: {}'.format(voicemailId))
                    logging.error(e)

            if deviceIds:
                for deviceId in deviceIds:
                    try:
                        self.kazooCli.delete_device(accountId, deviceId)
                    except Exception as e:
                        logging.error('Unable to delete device: {}'.format(deviceId))
                        logging.error(e)


            if phoneNumber is not None:
                try:
                    phoneNumber = phoneNumber[2:] if phoneNumber.startswith("+1") else phoneNumber
                    self.kazooCli.delete_phone_number(accountId, phoneNumber)
                except Exception as e:
                    logging.error('Unable to delete phone number: {}'.format(phoneNumber))
                    logging.error(e)

            try:
                self.kazooCli.delete_user(accountId, userId)
            except Exception as e:
                logging.error('Unable to delete userId: {}'.format(userId))
                logging.error(e)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')