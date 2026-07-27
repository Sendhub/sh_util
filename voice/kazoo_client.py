"""
Kazoo Client Module

This module provides a client interface for interacting with the Kazoo API. It includes
methods for managing accounts, users, devices, phone numbers, call flows, and other
Kazoo resources.

"""

import logging
import os
import tempfile
import urllib.parse
from copy import deepcopy
from io import BytesIO
from os.path import basename

import settings

from ..retry import retry
from ..sh_http.wget import wget

DEFAULT_RING_TIMEOUT = 30
DEFAULT_KAZOO_CALL_FLOW = {
    "numbers": [],
    "flow": {"module": "user", "data": {"id": "", "timeout": DEFAULT_RING_TIMEOUT, "can_call_self": False}, "children": {"_": {"module": "voicemail", "data": {"id": ""}, "children": {}}}},
}

NO_MATCH_CALL_FLOW = {"featurecode": {}, "numbers": ["no_match"], "flow": {"children": {}, "data": {}, "module": "offnet"}}


class KazooClient:
    """
    Kazoo Client Class

    This class provides methods to interact with the Kazoo API for managing accounts,
    users, devices, phone numbers, and other resources.

    Attributes:
        kazooCli: Kazoo client instance.
        redisCli: Redis client instance.
        authTokenCacheKey: Cache key for storing the authentication token.
        authToken: Authentication token for Kazoo API.
        ttl: Time-to-live for the authentication token cache.
    """

    kazoo_cli = settings.KAZOO_CLI
    redis_cli = settings.REDIS
    auth_token_cache_key = "kazooAuthToken"
    auth_token = None
    ttl = int(settings.KAZOO_AUTH_TOKEN_CACHE_EXPIRY_SECONDS)

    def __init__(self):
        """
        Initializes the KazooClient instance.

        Authenticates with the Kazoo API and caches the authentication token.
        """

        try:
            self.auth_token = self.redis_cli.get(self.auth_token_cache_key)

            if self.auth_token is None:
                self.auth_token = self.kazoo_cli.authenticate()
                logging.info("Authenticated against Kazoo. Caching result.")
                logging.info(f"Key: {self.auth_token_cache_key}")
                logging.info(f"AuthToken: {self.auth_token}")
                logging.info(f"settings.KAZOO_AUTH_TOKEN_CACHE_EXPIRY_SECONDS: {self.ttl} and type: {type(self.ttl)}")
                self.redis_cli.setex(name=self.auth_token_cache_key, value=self.auth_token, time=self.ttl)
            else:
                logging.info("Using cached Kazoo authentication")
                self.kazoo_cli.auth_token = self.auth_token
                self.kazoo_cli._authenticated = True
        except Exception as e:
            logging.error(f"Unable to authenticate on Kazoo: {str(e)}")
            self.auth_token = None
            import traceback

            traceback.print_exc(e)

    def create_enterprise_account(self, enterprise_id, name):
        """
        Creates an enterprise account on Kazoo.

        Args:
            enterpriseId (str): The unique ID of the enterprise account.
            name (str): The name of the enterprise account.

        Returns:
            dict: The result of the account creation.
        """  # Import moved here to avoid missing package dependency
        import kazoo.exceptions as exceptions

        logging.info(f"createEnterpriseAccount invoked with {enterprise_id}, {name}")

        if enterprise_id is None or name is None:
            raise exceptions.KazooApiError(f"EnterpriseId {enterprise_id} and Name {name} must be provided")

        result = {}

        @retry(3)
        def _wrapped_account_creation(result):
            """
            Wraps calls to account creation to allow for retries.

            Args:
                result (dict): The result dictionary to update.

            Returns:
                bool: True if account creation is successful, False otherwise.
            """

            result.update(self.kazoo_cli.create_account({"name": str(enterprise_id), "enterprise_id": str(enterprise_id), "enterprise_name": name, "realm": f"{enterprise_id}.sip.sendhub.com"}))

            return "data" in result and "id" in result["data"]

        if _wrapped_account_creation(result):
            logging.info(f"Created account {enterprise_id} successfully. Kazoo id = {result['data']['id']}")

            # Create the no-match call flow for this account
            self.kazoo_cli.create_callflow(result["data"]["id"], deepcopy(NO_MATCH_CALL_FLOW))
        else:
            logging.error(f"Unable to create account on Kazoo: {result}")
            raise RuntimeError(f"Kazoo account creation error: {result}")

        return result

    def get_user(self, account_id, kazoo_user_id):
        """
        Retrieves a user from Kazoo.

        Args:
            accountId (str): The account ID in Kazoo.
            kazooUserId (str): The user ID in Kazoo.

        Returns:
            dict: The user details retrieved from Kazoo.
        """
        # Import moved here to avoid missing package dependency
        import kazoo.exceptions as exceptions

        if account_id is None or kazoo_user_id is None:
            raise exceptions.KazooApiError(f"accountId {account_id} and kazooUserId {kazoo_user_id} must be provided")

        result = self.kazoo_cli.get_user(account_id, kazoo_user_id)

        return result

    def _soft_phone_template(self, owner_id, username, password):
        return {
            "name": f"{username}",
            "sip": {
                "method": "password",
                "username": username,
                "password": password,
            },
            "device_type": "softphone",
            "owner_id": str(owner_id),
        }

    def _physical_phone_template(self, owner_id, number, type="cellphone"):
        return {
            "name": number,
            "device_type": type,
            "call_forward": {"enabled": True, "substitute": True, "require_keypress": False, "keep_caller_id": True, "direct_calls_only": False, "ignore_early_media": True, "number": number},
            "media": {"bypass_media": "auto", "ignore_early_media": True},
            "owner_id": str(owner_id),
            "forwarding_number": number,
        }

    def list_devices(self, account_id, owner_id):
        """
        Lists devices for a specific owner in an account.

        Args:
            accountId (str): The account ID in Kazoo.
            ownerId (str): The owner ID whose devices are to be listed.

        Returns:
            list: A list of devices associated with the owner.
        """

        from kazoo.client import KazooClient

        request = KazooClient("/accounts/{account_id}/devices", get_params={"filter_owner_id": owner_id})
        request.auth_required = True

        return self.kazoo_cli._execute_request(request, account_id=account_id)

    def create_device(self, type, account_id, user_id, owner_id, number, username="", password=""):
        """
        Creates a device in Kazoo.

        Args:
            type (str): The type of device ('softphone' or 'cellphone').
            accountId (str): The account ID in Kazoo.
            userId (str): The user ID in Kazoo.
            ownerId (str): The owner ID for the device.
            number (str): The phone number associated with the device.
            username (str, optional): The username for the device. Defaults to ''.
            password (str, optional): The password for the device. Defaults to ''.

        Returns:
            dict or None: The created device details or None if creation fails.
        """

        assert type in ("softphone", "cellphone")
        import kazoo.exceptions as exceptions

        from sh_util.tel import validate_phone_number

        logging.info(f"createDevice invoked with type={type}, accountId={account_id}, userId={user_id}, ownerId={owner_id}, username={username}, password={password}")

        if validate_phone_number(number) is False:
            logging.warning(f"Phone number validation failed for accountId={account_id}, userId={user_id}, number={number}")
            return None

        if type == "softphone":
            device_params = self._soft_phone_template(owner_id, username, password)
        else:
            device_params = self._physical_phone_template(owner_id, number)

        try:
            return self.kazoo_cli.create_device(account_id, device_params)
        except exceptions.KazooApiBadDataError as e:
            if ("sip.username" in e.field_errors and "unique" in e.field_errors["sip.username"]) is False:
                logging.error(f"Unexpected error creating device: {str(e)}")
                raise
            logging.info(f"SIP Device already exists for username: {username}")

        return None

    def create_phone_number(self, account_id, number):
        """
        Creates a phone number in Kazoo.

        Args:
            accountId (str): The account ID in Kazoo.
            number (str): The phone number to be created.

        Returns:
            dict: The result of the phone number creation.
        """

        result = {}

        @retry(3)
        def _wrapped_number_creation(result, short_number):
            """
            Wraps calls to phone number creation to allow for retries.

            Args:
                result (dict): The result dictionary to update.
                shortNumber (str): The short version of the phone number.

            Returns:
                bool: True if phone number creation is successful, False otherwise.
            """

            logging.info(f"Creating phone number on Kazoo account={account_id}, number={short_number}")

            try:
                result.update(self.kazoo_cli.create_phone_number(account_id, short_number))

                logging.info(f"Phone number creation result: {'data' in result and 'id' in result['data']}")

                return "data" in result and "id" in result["data"]

            except Exception as e:
                logging.warning(f"Phone number creation threw exception: {e}")

            return False

        short_number = number[2:] if number.startswith("+1") else number
        _wrapped_number_creation(result, short_number)
        return result

    def provision_phone_number_and_add_to_call_flow(self, account_id, call_flow_id, number):
        """
        Provisions a phone number and adds it to a call flow in Kazoo.

        Args:
            accountId (str): The account ID in Kazoo.
            callFlowId (str): The call flow ID in Kazoo.
            number (str): The phone number to be provisioned.
        """

        logging.info(f"provisionPhoneNumberAndAddToCallFlow invoked with accountId={account_id}, callFlowId={call_flow_id}, number={number}")

        call_flow = self.kazoo_cli.get_callflow(account_id, call_flow_id)

        assert "data" in call_flow and "numbers" in call_flow["data"], "Detected invalid call flow when provisioning new number"

        result = self.create_phone_number(account_id, number)

        if "data" in result and "id" in result["data"]:
            call_flow["data"]["numbers"].append(number)
            self.kazoo_cli.update_callflow(account_id, call_flow_id, call_flow["data"])

    def de_provision_phone_number_and_remove_from_call_flow(self, account_id, call_flow_id, number):
        """
        De-provisions a phone number and removes it from a call flow in Kazoo.

        Args:
            accountId (str): The account ID in Kazoo.
            callFlowId (str): The call flow ID in Kazoo.
            number (str): The phone number to be de-provisioned.
        """

        logging.info(f"deProvisionPhoneNumberAndRemoveFromCallFlow invoked with accountId={account_id}, callFlowId={call_flow_id}, number={number}")

        call_flow = self.kazoo_cli.get_callflow(account_id, call_flow_id)

        assert "data" in call_flow and "numbers" in call_flow["data"], "Detected invalid call flow when de-provisioning number"

        call_flow["data"]["numbers"] = [nbr for nbr in call_flow["data"]["numbers"] if number != nbr]

        self.kazoo_cli.update_callflow(account_id, call_flow_id, call_flow["data"])

        short_number = number[2:] if number.startswith("+1") else number
        self.kazoo_cli.delete_phone_number(account_id, short_number)

    def update_vm_box(self, account_id, vm_box_id, update_data):
        """
        Update a vmbox on Kazoo within an given account
        updateData is a dictionary of optional (specific) overwrites
        over current user data in Kazoo
        """
        # Import moved here to avoid missing package dependency
        import kazoo.exceptions as exceptions

        if account_id is None or vm_box_id is None or update_data is None:
            raise exceptions.KazooApiError(
                "accountId {} and vmBoxId {} and updateData {} must be provided".  # noqa
                format(account_id, vm_box_id, update_data)
            )  # noqa

        current_vm_box_res = self.kazoo_cli.get_voicemail_box(account_id, vm_box_id)
        if current_vm_box_res["status"] != "success":
            raise exceptions.KazooApiError(f"Failed to get user: accountId {account_id}, vmBoxId {vm_box_id}")  # noqa

        user_data = current_vm_box_res["data"]
        user_data.update(update_data)
        result = self.kazoo_cli.update_voicemail_box(account_id, vm_box_id, user_data)

        return result

    def update_menu(self, account_id, menu_id, user_id, media_id):
        """update menu"""
        self.kazoo_cli.update_menu(
            account_id,
            menu_id,
            {"name": str(user_id), "retries": 3, "timeout": "10000", "max_extension_length": "1", "media": {"exit_media": True, "greeting": media_id, "invalid_media": True, "transfer_media": True}},
        )

    def copy_media(self, account_id, media_id, from_url):
        """copy media"""
        # Import moved here to avoid missing package dependency
        import kazoo.exceptions as exceptions
        import pycurl

        # this function doesn't fit the general model for crossbar
        # API URLs hence why it is hand built
        try:
            c = None
            fh = None

            media_data = wget(from_url, num_tries=3)

            toUrl = f"{self.kazoo_cli.base_url}/accounts/{account_id}/media/{media_id}/raw"  # noqa

            fh = tempfile.NamedTemporaryFile(mode="w+b")
            fh.write(media_data)
            fh.flush()
            fh.seek(0)

            c = pycurl.Curl()
            c.setopt(pycurl.URL, toUrl)
            c.setopt(pycurl.READFUNCTION, fh.read)
            c.setopt(pycurl.POST, 1)
            c.setopt(pycurl.HTTPHEADER, ["Content-type: audio/mp3", f"X-Auth-Token: {self.kazoo_cli.auth_token}"])
            c.setopt(pycurl.POSTFIELDSIZE, os.path.getsize(fh.name))
            response = BytesIO()
            c.setopt(c.WRITEFUNCTION, response.write)

            logging.info("Uploading file %s to url %s", str(fh.name), str(toUrl))

            c.perform()
            return_code = c.getinfo(pycurl.HTTP_CODE)
            logging.info("File upload %s Http %d Response %s", str(fh.name), int(return_code), str(response.getvalue()))
            if return_code != 200:
                raise exceptions.KazooApiError("Failed upload media, return code %d" % return_code)  # noqa

        finally:
            if c is not None:
                c.close()
            if fh is not None:
                fh.close()

    def add_media(self, account_id, url, name):
        """add media"""
        logging.info("Adding media %s-%s to account %s on Kazoo", str(name), str(url), str(account_id))

        result = None

        try:
            filename = basename(urllib.parse.urlparse(url).path)
            result = self.kazoo_cli.create_media(account_id, {"streamable": True, "name": name, "description": f"C:\\fakepath\\{filename}"})

            self.copy_media(account_id, result["data"]["id"], url)

        except Exception as e:
            logging.warning("Unable to create media %s-%s on account: %s", str(name), str(url), str(account_id))
            logging.warning(e)
            raise

        return result

    def delete_media(self, account_id, media_id):
        """delete media"""
        logging.info("Deleting media %s from account %s on Kazoo", str(media_id), str(account_id))

        result = None
        try:
            result = self.kazoo_cli.delete_media(account_id, media_id)

        except Exception as e:
            logging.warning("Unable to delete media %s from account: %s", str(media_id), str(account_id))
            logging.warning(e)
            raise

        return result

    def add_tts_media(self, account_id, tts, name):
        """add tts media"""
        logging.info("Adding tts media %s-%s to account %s on Kazoo", str(name), str(tts), str(account_id))

        result = None

        try:
            result = self.kazoo_cli.create_media(account_id, {"streamable": True, "name": name, "media_source": "tts", "tts": {"text": tts, "voice": "female/en-US"}})

        except Exception as e:
            logging.warning("Unable to create media %s-%s on account: %s", str(name), str(tts), str(account_id))
            logging.warning(e)
            raise

        return result

    def update_temporal_rules(self, account_id, rule_id, user_id, open_second, close_second, days_of_week):
        """update temporal rules"""
        self.kazoo_cli.update_temporal_rule(
            account_id,
            rule_id,
            {
                "name": str(user_id),  # noqa
                "time_window_start": open_second,
                "time_window_stop": close_second,
                "wdays": days_of_week,
                "name": f"{str(user_id)}",  # noqa
                "cycle": "weekly",
                "start_date": 62586115200,
                "ordinal": "every",
                "interval": 1,
            },
        )

    def update_call_flow(self, account_id, call_flow_id, call_flow_data):
        """update call flow"""
        logging.info("Updating callflow %s on account %s with data %s", str(call_flow_id), str(account_id), str(call_flow_data))

        self.kazoo_cli.update_callflow(account_id, call_flow_id, call_flow_data)

    def add_device_to_group(self, account_id, group_id, device_id, user_id):
        """add device to group"""
        result = self.kazoo_cli.get_group(account_id, group_id)

        if "data" in result and "endpoints" in result["data"]:
            endpoints = result["data"]["endpoints"]

            if device_id not in endpoints:
                endpoints.update({device_id: {"type": "device"}})

                self.kazoo_cli.update_group(
                    account_id,
                    group_id,
                    {
                        "music_on_hold": {},
                        "name": str(user_id),
                        "check_if_owner": True,
                        "require_pin": False,
                        "delete_after_notify": True,
                    },
                )

    def create_user(self, account_id, name, user_id, password, enterprise_id, sip_username, sip_password, soft_phone_number=None, cell_phone_numbers=[], email=None):
        """
        Create a user on Kazoo within an given enterprise or within the general
        sendhub enterprise

        accountId: Account on kazoo which this user will be created under
        name: ShUser name
        user_id: Id of the user
        password: Password to set on kazoo
        enterpriseId: The id of the enterprise account. The account must
        already exist on kazoo.
        sipUsername: SIP device username for the web device
        sipPassword: SIP password for the web device
        softPhoneNumber: Voip number too add for this account
        cellPhoneNumbers: Cell phone numbers to add for this account
        email: Email address for this account (will be set to a unique-bogus
        email if not specified as kazoo requires it)
        """

        logging.info(
            "createUser invoked with %s,%s,%s,%s,%s,%s,%s,%s", str(account_id), str(name), str(user_id), str(password), str(enterprise_id), str(sip_username), str(soft_phone_number), str(cell_phone_numbers)
        )

        user_details = {
            "id": None,
            "first_name": None,
            "username": None,
            "voicemailId": None,
            "softphoneId": None,
            "cellphoneIds": [],
            "callFlowId": None,
            "autoAttendantMenuId": None,
            "temporalRuleId": None,
        }

        short_soft_phone_number = None

        # Import moved here to avoid missing package dependency
        import kazoo.exceptions as exceptions

        if name is None or user_id is None or password is None:
            raise exceptions.KazooApiError(f"user_id {user_id} and Name {name} must be provided")  # noqa

        create_user_result = None
        try:
            user_settings = {
                "first_name": name,
                "last_name": "SH",
                "username": str(user_id),
                "password": password,
                "enterprise_id": str(enterprise_id),
                "email": f"{email}@no-reply.sendhub.com" if email is None else email,  # noqa
                "vm_to_email_enabled": False,
            }

            if soft_phone_number is not None:
                short_soft_phone_number = soft_phone_number[2:] if soft_phone_number.startswith("+1") else soft_phone_number
                caller_id = {"caller_id": {"internal": {"name": name, "number": short_soft_phone_number}, "external": {"name": name, "number": short_soft_phone_number}}}
                user_settings.update(caller_id)

            create_user_result = self.kazoo_cli.create_user(account_id, user_settings)

            if create_user_result["status"] == "success":
                user_details["id"] = create_user_result["data"]["id"]
                user_details["name"] = create_user_result["data"]["first_name"]
                user_details["username"] = create_user_result["data"]["username"]
                user_details["enterpriseId"] = create_user_result["data"]["enterprise_id"]

                call_flow = deepcopy(DEFAULT_KAZOO_CALL_FLOW)

                soft_phone_device_result = None
                if soft_phone_number is not None:
                    create_number_result = self.create_phone_number(account_id, short_soft_phone_number)

                    if "data" not in create_number_result or "id" not in create_number_result["data"]:
                        raise exceptions.KazooApiError(f"Unable to create phone number: {short_soft_phone_number}")

                    call_flow["numbers"].append(soft_phone_number)

                    soft_phone_device_result = self.create_device(
                        type="softphone", account_id=account_id, user_id=user_id, owner_id=user_details["id"], number=short_soft_phone_number, username=sip_username, password=sip_password
                    )

                    user_details["softphoneId"] = soft_phone_device_result["data"]["id"] if soft_phone_device_result is not None else None

                call_flow["numbers"].append(str(user_id))
                call_flow["flow"]["data"]["id"] = str(user_details["id"])

                cell_phone_results = []
                for number in cell_phone_numbers:
                    if number is not None:
                        short_number = number[2:] if number.startswith("+1") else number
                        cell_phone_result = self.create_device(type="cellphone", account_id=account_id, user_id=user_id, owner_id=user_details["id"], number=short_number)
                        if cell_phone_result is not None:
                            cell_phone_results.append(cell_phone_result)
                user_details["cellphoneIds"] = [
                    {"id": cell_phone_result["data"]["id"], "number": "+1{}".format(cell_phone_result["data"]["call_forward"]["number"])} for cell_phone_result in cell_phone_results
                ]  # noqa

                # the following requires that the schema be changed on kazoo.
                # so if this fails, then check
                vm_box_obj = self.kazoo_cli.create_voicemail_box(
                    account_id, {"mailbox": str(user_id), "check_if_owner": True, "require_pin": False, "name": str(user_id), "delete_after_notify": True, "owner_id": str(user_details["id"])}
                )
                user_details["voicemailId"] = vm_box_obj["data"]["id"]
                call_flow["flow"]["children"]["_"]["data"]["id"] = user_details["voicemailId"]

                call_flow_result = self.kazoo_cli.create_callflow(account_id, call_flow)
                user_details["callFlowId"] = call_flow_result["data"]["id"]

                auto_attendant_menu_result = self.kazoo_cli.create_menu(account_id, {"name": str(user_id), "retries": 3, "timeout": "10000", "max_extension_length": "1"})
                user_details["autoAttendantMenuId"] = auto_attendant_menu_result["data"]["id"]

                temporal_rule_result = self.kazoo_cli.create_temporal_rule(
                    account_id,
                    {
                        "name": str(user_id),  # noqa
                        "time_window_start": 0,
                        "time_window_stop": 86400,
                        "wdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
                        "name": f"{str(user_id)}",  # noqa
                        "cycle": "weekly",
                        "start_date": 62586115200,
                        "ordinal": "every",
                        "interval": 1,
                    },
                )

                user_details["temporalRuleId"] = temporal_rule_result["data"]["id"]

        except Exception as e:
            logging.error("Unable to create user on Kazoo: %s", str(e))
            import traceback

            traceback.print_exc(e)

            # if we couldn't create the user then try to delete them so
            # we can try again
            if create_user_result is not None and create_user_result["status"] == "success":
                logging.error("Deleting partially created user")
                self.delete_user(
                    account_id,
                    user_details["id"],
                    short_soft_phone_number,
                    user_details["cellphoneIds"].extend([user_details["softphoneId"]]),  # noqa
                    user_details["voicemailId"],
                    user_details["callFlowId"],
                    user_details["autoAttendantMenuId"],
                    user_details["temporalRuleId"],
                )
            raise

        return user_details

    def update_user(self, account_id, kazoo_user_id, update_data):
        """
        Update a user on Kazoo within an given account
        updateData is a dictionary of optional (specific) overwrites over
        current user data in Kazoo
        """
        # Import moved here to avoid missing package dependency
        import kazoo.exceptions as exceptions

        if account_id is None or kazoo_user_id is None or update_data is None:
            raise exceptions.KazooApiError("accountId {} and kazooUserId {} and updateData {} must be provided".format(account_id, kazoo_user_id, update_data))  # noqa

        current_user_res = self.kazoo_cli.get_user(account_id, kazoo_user_id)
        if current_user_res["status"] != "success":
            raise exceptions.KazooApiError(f"Failed to get user: accountId {account_id}, kazooUserId {kazoo_user_id}")  # noqa

        user_data = current_user_res["data"]
        user_data.update(update_data)
        result = self.kazoo_cli.update_user(account_id, kazoo_user_id, user_data)

        return result

    def delete_account(self, account_id):
        """delete account"""
        logging.info("Deleting account %s on Kazoo", str(account_id))

        try:
            self.kazoo_cli.delete_account(account_id)
        except Exception as e:
            logging.error("Unable to delete account: %s", str(account_id))
            logging.error(e)

    def delete_user(self, account_id, user_id, phone_number=None, device_ids=[], voicemail_id=None, call_flow_id=None, menu_id=None, temporal_rule_id=None):
        """delete user"""
        logging.info("Deleting user on Kazoo with account %s and user %s", str(account_id), str(user_id))

        if menu_id is not None:
            try:
                self.kazoo_cli.delete_menu(account_id, menu_id)
            except Exception as e:
                logging.warning("Unable to delete menu: %s", str(menu_id))
                logging.warning(e)

        if temporal_rule_id is not None:
            try:
                self.kazoo_cli.delete_temporal_rule(account_id, temporal_rule_id)
            except Exception as e:
                logging.warning("Unable to delete temporal rule: %s", str(temporal_rule_id))
                logging.warning(e)

        if call_flow_id is not None:
            try:
                self.kazoo_cli.delete_callflow(account_id, call_flow_id)
            except Exception as e:
                logging.warning("Unable to delete callflow: %s", str(call_flow_id))
                logging.warning(e)

        if voicemail_id is not None:
            try:
                self.kazoo_cli.delete_voicemail_box(account_id, voicemail_id)
            except Exception as e:
                logging.warning("Unable to delete vm: %s", str(voicemail_id))
                logging.warning(e)

        if device_ids:
            for device_id in device_ids:
                try:
                    self.kazoo_cli.delete_device(account_id, device_id)
                except Exception as e:
                    logging.warning("Unable to delete device: %s", str(device_id))
                    logging.warning(e)

        if phone_number is not None:
            try:
                phone_number = phone_number[2:] if phone_number.startswith("+1") else phone_number
                self.kazoo_cli.delete_phone_number(account_id, phone_number)
            except Exception as e:
                logging.warning("Unable to delete phone number: %s", str(phone_number))
                logging.warning(e)

        try:
            if user_id is not None:
                self.kazoo_cli.delete_user(account_id, user_id)
        except Exception as e:
            logging.warning("Unable to delete user_id: %s", str(user_id))
            logging.warning(e)
