
import  re
import itertools

# Matches the Build number in the user agent string.

_client_app_build_number_re = re.compile(r'AppVersion\:\:(?P<versionName>[^\s:]+) \((?P<buildNumber>[0-9]+)\)$', re.IGNORECASE)
_client_app_platform_type_re = re.compile(r'Platform\:\:(?P<platformType>[a-z\s]+)/', re.IGNORECASE)
_client_ios_platform_type = re.compile(r'iOS|iPhone|iPod|iPad', re.IGNORECASE)
_client_android_platform_type = re.compile(r'Android', re.IGNORECASE)

def get_sendhub_user_agent_string(request):
    sh_user_agent_str = None
    if (hasattr(request, 'META') and 'HTTP_X_SH_USER_AGENT' in request.META):
        sh_user_agent_str = request.META.get('HTTP_X_SH_USER_AGENT', None)
    return sh_user_agent_str

def get_sendhub_user_agent_props(request):
    """
    Converts the SendHub User Agent String into a Usable Dictionary

    :param request: containing the SendHub User Agent String
        example: 'Platform::iOS/OSVersion::6.1/AppVersion::2.9TF (0134)'
    :return: A dictionary Mapping the Keys to values
        example: {'AppVersion': '2.9TF (0134)', 'OSVersion': '6.1', 'Platform': 'iOS'}
    """
    agent_str = get_sendhub_user_agent_string(request)
    props = {}
    if agent_str is not None:
        prop_pairs = agent_str.split('/')
        props_serial = []
        # split on "::" and append to the list
        [props_serial.extend(pair.split('::')) for pair in prop_pairs]
        # grouper recipe: http://docs.python.org/2/library/itertools.html#recipes
        props = dict(itertools.zip_longest(*[iter(props_serial)] * 2, fillvalue=""))
    return props


def get_client_app_build_number(request):
    sh_user_agent_str = get_sendhub_user_agent_string(request)
    build_number = -1
    if sh_user_agent_str is not None:
        matches = _client_app_build_number_re.search(sh_user_agent_str)
        if matches is not None:
            build_number = int(matches.group('buildNumber'))

    return build_number


def get_client_platform_type(request):
    sh_user_agent_str = get_sendhub_user_agent_string(request)
    platform_type = 'web'

    if sh_user_agent_str is not None:
        matches = _client_app_platform_type_re.search(sh_user_agent_str)
        if matches is not None:
            platform_type = matches.group('platformType')
            if _client_ios_platform_type.match(platform_type):
                platform_type = 'ios'
            elif _client_android_platform_type.match(platform_type):
                platform_type = 'android'


    return platform_type