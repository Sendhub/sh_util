import settings
from sh_util.retry import retry
import kazoo.exceptions as exceptions
import logging
from requests.exceptions import RequestException
import tempfile
from sh_util.http.wget import wget
import pycurl
import os
import cStringIO
from urlparse import urlparse
from os.path import basename
from copy import deepcopy

DEFAULT_RING_TIMEOUT = 30
DEFAULT_KAZOO_CALL_FLOW = {
    'numbers':[],
    'flow':{
        'module':'user',
        'data':{
            'id':'',
            'timeout': DEFAULT_RING_TIMEOUT,
            'can_call_self': False
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

        logging.info(u'createEnterpriseAccount invoked with {},{}'.format(enterpriseId, name))


        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:

            if enterpriseId is None or name is None:
                raise exceptions.KazooApiError(u'EnterpriseId {} and Name {} must be provided'.format(enterpriseId, name))

            result = {}

            @retry(3)
            def _wrappedAccountCreation(result):
                '''
                Wrap calls to account creation to allow for retries
                '''

                result.update(self.kazooCli.create_account(
                    {
                        u'name':str(enterpriseId),
                        u'enterprise_id':str(enterpriseId),
                        u'enterprise_name':name,
                        u'realm':u'{}.sip.sendhub.com'.format(enterpriseId)
                    }
                ))

                return ('data' in result and 'id' in result['data'])

            if _wrappedAccountCreation(result):
                logging.info('Created account {} successfully. Kazoo id = {}'.format(enterpriseId, result['data']['id']))

                # create the no-match call flow for this account
                # so the global carrier stuff works
                self.kazooCli.create_callflow(result['data']['id'], deepcopy(NO_MATCH_CALL_FLOW))
            else:
                logging.error('Unable to create account on kazoo: {}'.format(result))

                raise Exception('Kazoo account creation error: {}'.format(result))
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
            u'name':'{}-{}'.format(number, username),
            u'sip': {
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
            u'device_type':u'softphone',
            u'owner_id':str(ownerId)
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
                u'require_keypress': False,
                u'keep_caller_id': True,
                u'direct_calls_only': False,
                u'ignore_early_media': True,
                u'number': number
            },
            u"media": {
                u"bypass_media": u"auto",
                u"ignore_early_media": True
            },
            u'owner_id':str(ownerId),
            u'forwarding_number':number
        }

    def listDevices(self, accountId, ownerId):
        from kazoo.client import KazooRequest

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if not authenticated:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        request = KazooRequest("/accounts/{account_id}/devices", get_params={
            "filter_owner_id": ownerId
        })
        request.auth_required = True

        return self.kazooCli._execute_request(request, account_id=accountId)

    def createDevice(self, type, accountId, userId, enterpriseId, ownerId, number, username=u'', password=u''):
        assert type in (u'softphone', u'cellphone', u'landline')
        from sh_util.tel import validatePhoneNumber

        logging.info(u'createDevice invoked with {},{},{},{},{},{},{}'.format(type, accountId, userId, enterpriseId, ownerId, number, username))

        if validatePhoneNumber(number) is False:
            logging.warning(u'Phone number validation failed for {}-{}-{}'.format(accountId, userId, number))
            return None

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
            logging.info(u'SIP Device already exists for username: {}'.format(username))

        return None


    def createPhoneNumber(self, accountId, number):
        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:


            result = {}

            @retry(3)
            def _wrappedNumberCreation(result, shortNumber):
                '''
                Wrap calls to account creation to allow for retries
                '''

                logging.info('Creating phone number on kazoo account={}, number={}'.format(accountId, shortNumber))

                try:
                    result.update(self.kazooCli.create_phone_number(accountId, shortNumber))

                    logging.info('Phone number creation result: {}'.format('data' in result and 'id' in result['data']))

                    return ('data' in result and 'id' in result['data'])

                except Exception as e:
                    logging.warning('Phone number creation threw exception: {}'.format(e))

                return False

            shortNumber = number[2:] if number.startswith("+1") else number
            _wrappedNumberCreation(result, shortNumber)
            return result

        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def provisionPhoneNumberAndAddToCallFlow(self, accountId, callFlowId, number):

        logging.info(u'provisionPhoneNumberAndAddToCallFlow invoked with {},{},{}'.format(accountId, callFlowId, number))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            # let this blow up if it fails.. it should always succeed
            callFlow = self.kazooCli.get_callflow(accountId, callFlowId)

            # anything but the following is invalid, so this should blow up
            assert 'data' in callFlow and 'numbers' in callFlow['data'], "Detected invalid call flow when provisioning new number"

            result = self.createPhoneNumber(accountId, number)

            if 'data' in result and 'id' in result['data']:
                callFlow['data']['numbers'].append(number)
                self.kazooCli.update_callflow(accountId, callFlowId, callFlow['data'])
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def deProvisionPhoneNumberAndRemoveFromCallFlow(self, accountId, callFlowId, number):
        logging.info(u'deProvisionPhoneNumberAndRemoveFromCallFlow invoked with {},{},{}'.format(accountId, callFlowId, number))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            # let this blow up if it fails.. it should always succeed
            callFlow = self.kazooCli.get_callflow(accountId, callFlowId)

            # anything but the following is invalid, so this should blow up
            assert 'data' in callFlow and 'numbers' in callFlow['data'], "Detected invalid call flow when provisioning new number"

            callFlow['data']['numbers'] = [nbr for nbr in callFlow['data']['numbers'] if number != nbr]

            self.kazooCli.update_callflow(accountId, callFlowId, callFlow['data'])

            shortNumber = number[2:] if number.startswith("+1") else number
            self.kazooCli.delete_phone_number(accountId, shortNumber)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def updateVmBox(self, accountId, vmBoxId, ownerId, userId, mediaId):
        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            self.kazooCli.update_voicemail_box(
                accountId,
                vmBoxId,
                {
                    'check_if_owner': True,
                    'mailbox':str(userId),
                    'require_pin':False,
                    'name':str(userId),
                    'check_if_owner': True,
                    'owner_id':str(ownerId),
                    'skip_instructions': True,
                    'media':{
                        'unavailable':mediaId
                    }
                }
            )
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def updateMenu(self, accountId, menuId, userId, mediaId):
        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            self.kazooCli.update_menu(
                accountId,
                menuId,
                {
                    'name':str(userId),
                    'retries' : 3,
                    'timeout' : '10000',
                    'max_extension_length':'1',
                    'media' : {
                        'exit_media': True,
                        'greeting': mediaId,
                        'invalid_media': True,
                        'transfer_media': True
                    }
                }
            )
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def copyMedia(self, accountId, mediaId, fromUrl):

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            # this function doesn't fit the general model for crossbar API URLs hence why it is hand built
            try:
                c = None
                fh = None

                mediaData = wget(fromUrl, numTries=3)

                toUrl = '{}/accounts/{}/media/{}/raw'.format(self.kazooCli.base_url, accountId, mediaId)

                fh = tempfile.NamedTemporaryFile(mode='wr+b')
                fh.write(mediaData)
                fh.flush()
                fh.seek(0)

                c = pycurl.Curl()
                c.setopt(pycurl.URL, toUrl)
                c.setopt(pycurl.READFUNCTION, fh.read)
                c.setopt(pycurl.POST, 1)
                c.setopt(pycurl.HTTPHEADER, ["Content-type: audio/mp3", "X-Auth-Token: {}".format(self.kazooCli.auth_token)])
                c.setopt(pycurl.POSTFIELDSIZE, os.path.getsize(fh.name))
                response = cStringIO.StringIO()
                c.setopt(c.WRITEFUNCTION, response.write)

                logging.info(u'Uploading file %s to url %s' % (fh.name, toUrl))

                c.perform()
                returnCode = c.getinfo(pycurl.HTTP_CODE)
                logging.info("File upload %s Http %d Response %s" % (fh.name, returnCode, response.getvalue()))
                if returnCode != 200:
                    raise exceptions.KazooApiError('Failed upload media, return code %d' % returnCode)

            finally:
                if c is not None:
                    c.close()
                if fh is not None:
                    fh.close()
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def addMedia(self, accountId, url, name):
        logging.info(u'Adding media {}-{} to account {} on Kazoo'.format(name, url, accountId))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        result = None
        if authenticated:
            try:
                filename = basename(urlparse(url).path)
                result = self.kazooCli.create_media(accountId, {'streamable':True, 'name':name, 'description':'C:\\fakepath\\{}'.format(filename)})

                self.copyMedia(accountId, result['data']['id'], url)

            except Exception as e:
                logging.warning('Unable to create media {}-{} on account: {}'.format(name, url, accountId))
                logging.warning(e)
                raise
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return result

    def deleteMedia(self, accountId, mediaId):
        logging.info(u'Deleting media {} from account {} on Kazoo'.format(mediaId, accountId))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        result = None
        if authenticated:
            try:
                result = self.kazooCli.delete_media(accountId, mediaId)

            except Exception as e:
                logging.warning('Unable to delete media {} from account: {}'.format(mediaId, accountId))
                logging.warning(e)
                raise
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return result

    def addTtsMedia(self, accountId, tts, name):
        logging.info(u'Adding tts media {}-{} to account {} on Kazoo'.format(name, tts, accountId))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        result = None
        if authenticated:
            try:
                result = self.kazooCli.create_media(accountId, {'streamable':True, 'name':name, "media_source":"tts","tts":{"text":tts,"voice":"female/en-US"}})

            except Exception as e:
                logging.warning('Unable to create media {}-{} on account: {}'.format(name, tts, accountId))
                logging.warning(e)
                raise
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return result

    def updateTemporalRules(self, accountId, ruleId, userId, openSecond, closeSecond, daysOfWeek):
        self.kazooCli.update_temporal_rule(
                accountId,
                ruleId,
                {
                    'name':str(userId),
                    'time_window_start':openSecond,
                    'time_window_stop':closeSecond,
                    'wdays':daysOfWeek,
                    'name': '{}'.format(str(userId)),
                    'cycle':'weekly',
                    'start_date':62586115200,
                    'ordinal':'every',
                    'interval':1
                }
            )

    def updateCallFlow(self, accountId, callFlowId, callFlowData):

        logging.info(u'Updating callflow {} on account {} with data {}'.format(callFlowId, accountId, callFlowData))

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            self.kazooCli.update_callflow(
                    accountId,
                    callFlowId,
                    callFlowData
                )
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

    def addDeviceToGroup(self, accountId, groupId, deviceId, userId):

        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:
            result = self.kazooCli.get_group(accountId, groupId)

            if 'data' in result and 'endpoints' in result['data']:

                endpoints = result['data']['endpoints']

                if deviceId not in endpoints:

                    endpoints.update({deviceId : {u"type": u"device"}})

                    self.kazooCli.update_group(
                        accountId,
                        groupId,
                        {
                            u"music_on_hold": {},
                            u"name": str(userId),
                            u"resources": {},
                            u"endpoints": endpoints
                        }
                    )
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

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

        logging.info(u'createUser invoked with {},{},{},{},{},{},{},{}'.format(accountId, name, userId, password, enterpriseId, sipUsername, softPhoneNumber, cellPhoneNumbers))


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
            'autoAttendantMenuId':None,
            'temporalRuleId':None
        }

        shortSoftPhoneNumber = None

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
                        u'email':u'{}@no-reply.sendhub.com'.format(email) if email is None else email,
                        u'vm_to_email_enabled': False,
                    }
                )

                if createUserResult['status'] == 'success':
                    userDetails['id'] = createUserResult['data']['id']
                    userDetails['name'] = createUserResult['data']['first_name']
                    userDetails['username'] = createUserResult['data']['username']
                    userDetails['enterpriseId'] = createUserResult['data']['enterprise_id']

                    callFlow = deepcopy(DEFAULT_KAZOO_CALL_FLOW)

                    softPhoneDeviceResult = None
                    if softPhoneNumber is not None:
                        shortSoftPhoneNumber = softPhoneNumber[2:] if softPhoneNumber.startswith("+1") else softPhoneNumber
                        createNumberResult = self.createPhoneNumber(accountId, shortSoftPhoneNumber)

                        if 'data' not in createNumberResult or 'id' not in createNumberResult['data']:
                            raise exceptions.KazooApiError(u'Unable to create phone number: {}'.format(shortSoftPhoneNumber))

                        callFlow['numbers'].append(softPhoneNumber)

                        softPhoneDeviceResult = self.createDevice(type=u'softphone', accountId=accountId, userId=userId, enterpriseId=enterpriseId,
                                          ownerId=userDetails['id'], number=shortSoftPhoneNumber, username=sipUsername, password=sipPassword)

                        userDetails['softphoneId'] = softPhoneDeviceResult['data']['id'] if softPhoneDeviceResult is not None else None

                    callFlow['numbers'].append(str(userId))
                    callFlow['flow']['data']['id'] = str(userDetails['id'])

                    cellPhoneResults = []
                    for number in cellPhoneNumbers:
                        if number is not None:
                            shortNumber = number[2:] if number.startswith("+1") else number
                            cellPhoneResult = self.createDevice(type=u'cellphone', accountId=accountId, userId=userId, enterpriseId=enterpriseId,
                                          ownerId=userDetails['id'], number=shortNumber)
                            if cellPhoneResult is not None:
                                cellPhoneResults.append(cellPhoneResult)
                    userDetails['cellphoneIds'] = [{'id':cellPhoneResult['data']['id'], 'number':'+1{}'.format(cellPhoneResult['data']['call_forward']['number'])} for cellPhoneResult in cellPhoneResults]


                    # the following requires that the schema be changed on kazoo.
                    # so if this fails, then check
                    vmBoxObj = self.kazooCli.create_voicemail_box(
                        accountId,
                        {
                            'mailbox':str(userId),
                            'check_if_owner': True,
                            'require_pin':False,
                            'name':str(userId),
                            'check_if_owner': True,
                            'delete_after_notify': True,
                            'owner_id':str(userDetails['id'])
                        }
                    )
                    userDetails['voicemailId'] = vmBoxObj['data']['id']
                    callFlow['flow']['children']['_']['data']['id'] = userDetails['voicemailId']

                    callFlowResult = self.kazooCli.create_callflow(accountId, callFlow)
                    userDetails['callFlowId'] = callFlowResult['data']['id']

                    autoAttendantMenuResult = self.kazooCli.create_menu(
                        accountId,
                        {
                            'name':str(userId),
                            'retries' : 3,
                            'timeout' : '10000',
                            'max_extension_length':'1'
                        }
                    )
                    userDetails['autoAttendantMenuId'] = autoAttendantMenuResult['data']['id']

                    temporalRuleResult = self.kazooCli.create_temporal_rule(
                        accountId,
                        {
                            'name':str(userId),
                            'time_window_start':0,
                            'time_window_stop':86400,
                            'wdays':[
                                'monday',
                                'tuesday',
                                'wednesday',
                                'thursday',
                                'friday',
                                'saturday',
                                'sunday'
                            ],
                            'name': '{}'.format(str(userId)),
                            'cycle':'weekly',
                            'start_date':62586115200,
                            'ordinal':'every',
                            'interval':1
                        }
                    )

                    userDetails['temporalRuleId'] = temporalRuleResult['data']['id']

            except Exception as e:

                logging.error(u'Unable to create user on Kazoo: {}'.format(e))
                import traceback
                traceback.print_exc(e)

                # if we couldn't create the user then try to delete them so
                # we can try again
                if createUserResult is not None and createUserResult['status'] == 'success':
                    logging.error(u'Deleting partially created user')
                    self.deleteUser(accountId, userDetails['id'], shortSoftPhoneNumber, userDetails['cellphoneIds'].extend([userDetails['softphoneId']]),
                                    userDetails['voicemailId'], userDetails['callFlowId'], userDetails['autoAttendantMenuId'],
                                    userDetails['temporalRuleId'])
                raise

        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return userDetails

    def updateUser(self, accountId, kazooUserId, updateData):
        '''
        Update a user on Kazoo within an given account
        updateData is a dictionary of optional (specific) overwrites over current user data in Kazoo
        '''
        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:

            if accountId is None or kazooUserId is None or updateData is None:
                raise exceptions.KazooApiError(u'accountId {} and kazooUserId {} and updateData {} must be provided'.
                                               format(accountId, kazooUserId, updateData))

            currentUserRes = self.kazooCli.get_user(accountId, kazooUserId)
            if currentUserRes['status'] != 'success':
                raise exceptions.KazooApiError(u'Failed to get user: accountId {}, kazooUserId {}'.format(accountId, kazooUserId))

            userData = currentUserRes['data']
            userData.update(updateData)
            result = self.kazooCli.update_user(accountId, kazooUserId, userData)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')

        return result

    def deleteAccount(self, accountId):
        logging.info(u'Deleting account {} on Kazoo'.format(accountId))

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

    def deleteUser(self, accountId, userId, phoneNumber=None, deviceIds=[], voicemailId=None, callFlowId=None, menuId=None, temporalRuleId=None):
        logging.info(u'Deleting user on Kazoo with account {} and user {}'.format(accountId, userId))


        if self.authToken is None:
            authenticated = self.authenticate()
        else:
            authenticated = True

        if authenticated:

            if menuId is not None:
                try:
                    self.kazooCli.delete_menu(accountId, menuId)
                except Exception as e:
                    logging.warning('Unable to delete menu: {}'.format(menuId))
                    logging.warning(e)

            if temporalRuleId is not None:
                try:
                    self.kazooCli.delete_temporal_rule(accountId, temporalRuleId)
                except Exception as e:
                    logging.warning('Unable to delete temporal rule: {}'.format(temporalRuleId))
                    logging.warning(e)

            if callFlowId is not None:
                try:
                    self.kazooCli.delete_callflow(accountId, callFlowId)
                except Exception as e:
                    logging.warning('Unable to delete callflow: {}'.format(callFlowId))
                    logging.warning(e)

            if voicemailId is not None:
                try:
                    self.kazooCli.delete_voicemail_box(accountId, voicemailId)
                except Exception as e:
                    logging.warning('Unable to delete vm: {}'.format(voicemailId))
                    logging.warning(e)

            if deviceIds:
                for deviceId in deviceIds:
                    try:
                        self.kazooCli.delete_device(accountId, deviceId)
                    except Exception as e:
                        logging.warning('Unable to delete device: {}'.format(deviceId))
                        logging.warning(e)


            if phoneNumber is not None:
                try:
                    phoneNumber = phoneNumber[2:] if phoneNumber.startswith("+1") else phoneNumber
                    self.kazooCli.delete_phone_number(accountId, phoneNumber)
                except Exception as e:
                    logging.warning('Unable to delete phone number: {}'.format(phoneNumber))
                    logging.warning(e)

            try:
                self.kazooCli.delete_user(accountId, userId)
            except Exception as e:
                logging.warning('Unable to delete userId: {}'.format(userId))
                logging.warning(e)
        else:
            raise exceptions.KazooApiError(u'Kazoo Authentication Error')