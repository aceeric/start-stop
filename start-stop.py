import boto3
import datetime
import traceback

SERVER1_NAME = "S1"
SERVER2_NAME = "S2"
SERVER1_INSTANCEID = "*******************"
SERVER2_INSTANCEID = "*******************"


server_instances = [
    {"name": SERVER1_NAME, "specs": {"region": "us-east-1b", "id": SERVER1_INSTANCEID}},
    {"name": SERVER2_NAME, "specs": {"region": "us-east-1d", "id": SERVER2_INSTANCEID}}
]

AWS_ACCESS_KEY_ID = "********************"
AWS_SECRET_ACCESS_KEY = "****************************************"


# for testing, and CloudWatch config, the messages that the lambda handler recognizes
run_job_action = {"action": "RunJobStream"}
start_server2_action = {"action": "StartServer2"}
stop_server2_action = {"action": "StopServer2"}
stop_all_action = {"action": "StopAll"}

# an invalid message for testing
invalid_action = {"action": "FOO!"}

SUNDAY = 6


def lambda_handler(event, context):
    """
    Lambda handler called by AWS CloudWatch

    :param event: Expected to be a dictionary object structured like the valid actions above will
     be passed to the do_work function
    :param context: As defined by AWS. Currently unused by the function

    :return: None
    """
    log("Instance start/stop lambda_handler: BEGIN")
    do_work(event)
    log("Instance start/stop lambda_handler: END")


def is_dst():
    """
    Figures out if it is presently daylight savings time

    :return: true if it is
    """
    present = datetime.datetime.now()
    wrk = datetime.datetime(present.year, 3, 1)

    # calc start of DST: 2:00 a.m. on the second Sunday of March
    sunday_cnt = 1 if wrk.weekday() == SUNDAY else 0
    while sunday_cnt != 2:
        wrk += datetime.timedelta(days=1)
        sunday_cnt += (1 if wrk.weekday() == SUNDAY else 0)
    dst_start = datetime.datetime.combine(wrk, datetime.time(hour=2, minute=0, second=0))
    # print("DST starts:" + str(dst_start))

    # calc end of DST: 2:00 a.m. on the first Sunday of November
    wrk = datetime.datetime(present.year, 11, 1)
    sunday_cnt = 1 if wrk.weekday() == SUNDAY else 0
    while sunday_cnt != 1:
        wrk += datetime.timedelta(days=1)
        sunday_cnt += (1 if wrk.weekday() == SUNDAY else 0)
    dst_end = datetime.datetime.combine(wrk, datetime.time(hour=2, minute=0, second=0))
    # print("DST ends:" + str(dst_end))

    # print("cur time:" + str(curdatetime))
    result = dst_start <= present <= dst_end
    # print("it is%s DST" % ("" if is_dst else " not"))
    return result


def do_work(event):
    """
    Main worker for the module

    :param event: as received from AWS. If this is not a Python dictionary in the format
     expected, then an error will be logged

    :return: None
    """
    try:
        action = event["action"]
        log("Action: " + action)

        # Central Daylight Saving Time = UTC - 5
        # Central Standard Time        = UTC - 6
        # CloudWatch  fires this event twice. Once at
        # 1400 UTC and once at 1500 UTC for start
        # and 1700 UTC and 1800 UTC for stop. This function
        # figures out whether it is standard or daylight
        # saving  time so that it can only run once. This way
        # the time change is automatically handled and the server
        # start/stop window is 9AM-12PM weekdays Central Time.
        if action in ["StartServer2", "StopServer2"]:
            isdst = is_dst()
            utc_hour = datetime.datetime.utcnow().hour
            central_hour = utc_hour - (5 if isdst else 6)
            log("utc_hour: " + str(utc_hour))
            log("isdst: " + str(isdst))
            log("central_hour: " + str(central_hour))
            if action == "StartServer2" and central_hour != 9:
                log("Skipping start - not 9 AM")
                return
            elif action == "StopServer2" and central_hour != 12:
                log("Skipping stop - not 12 Noon")
                return

        if action == "RunJobStream":
            servers = [SERVER1_NAME]
            if datetime.datetime.today().weekday() == SUNDAY:
                # also start second server
                servers.append(SERVER2_NAME)
            log("Starting Servers: " + str(servers))
            start_instances_by_name(*servers)
            log("Servers started")
        elif action == "StartServer2":
            log("Starting " + SERVER2_NAME)
            start_instances_by_name(SERVER2_NAME)
            log(SERVER2_NAME + " started")
        elif action == "StopServer2":
            log("Stopping " + SERVER2_NAME)
            stop_instances_by_name(SERVER2_NAME)
            log(SERVER2_NAME + " stopped")
        elif action == "StopAll":
            log("Stopping All Servers")
            stop_instances_by_name(SERVER2_NAME, SERVER2_NAME)
            log("Servers stopped")
        else:
            log("Unsupported action -- " + action + " -- passed to lambda handler", "ERR")
    except Exception:
        log("An exception occurred: " + traceback.format_exc(), "ERR")


def log(msg, level="INFO"):
    """
    Prints a message with a timestamp
    :param step: the message to print
    :param level: optional logging level ("ERROR", "INFO", "WARN"). If not supplied, then "INFO"
                  is used by the function
    :return: None
    """
    (dt, micro) = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
    dt = "%s.%03d" % (dt, int(micro) / 1000)
    print('[{}][{}] {}'.format(dt, level, msg))


def get_boto_ec2_client():
    """
    Gets a boto ec2 client instance

    :return: a boto ec2 client instance
    """
    ec2 = boto3.client(
        "ec2",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    return ec2


def get_boto_ec2_resource():
    """
    Gets a boto ec2 resource instance

    :return: a boto ec2 resource instance
    """
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    return ec2


def start_instances_by_name(*instance_names):
    """
    Starts all the instances in the passed variable arg list.

    :param str instance_names: variable arg list f instance names

    :return: None
    """
    for instance_name in instance_names:
        if not instance_name_is_running(instance_name):
            instance_id = get_instance_id(instance_name)
            start_instance(instance_id)
        else:
            log("Request was made to start instance, but it is already running: " + instance_name,
                "WARN")


def stop_instances_by_name(*instance_names):
    """
    Stops all the instances in the passed variable arg list

    :param str instance_names: variable arg list of instance names

    :return: None
    """
    for instance_name in instance_names:
        if instance_name_is_running(instance_name):
            instance_id = get_instance_id(instance_name)
            stop_instance(instance_id)
        else:
            log("Request was made to stop instance, but it is not running: " + instance_name,
                "WARN")


def get_instance_id(name):
    """
    Gets the instance ID from the server_instances data structure for the passed name.

    :param str name: The instance name for which id is to be retrieved. E.g. SERVER2

    :return: The instance ID
    """
    return get_instance_specs(name)["specs"]["id"]


def instance_name_is_running(name):
    """
    Determines if the instance identified by the passed name is running

    :param str name: The instance name for which specs are to be retrieved. E.g. SERVER2

    :return: True if the instance is running, else False. Also returns False if the passed instance
     name is invalid.
    """
    instance = get_instance_specs(name)
    running = False
    if instance is not None:
        instance_id = instance["specs"]["id"]
        running = instance_id_is_running(instance_id)
    return running


def instance_id_is_running(instance_id):
    """
    Determines whether an instance is running or not

    :param str instance_id: a single EC2 instance ID

    :return: True if the instance is running, else False. Also returns False if the passed instance
     id is invalid.
    """
    ec2 = get_boto_ec2_resource()
    running = False
    for instance in ec2.instances.all():
        if instance.instance_id == instance_id:
            if instance.state["Name"] == "running":
                running = True
                break
    return running


def stop_instance(instance_id):
    """
    Stops the passed EC2 instance

    :param str instance_id: a single EC2 instance ID

    :return: None
    """
    ec2 = get_boto_ec2_client()
    ec2.stop_instances(InstanceIds=[instance_id])
    log("Stopping Instance: " + str(instance_id))


def start_instance(instance_id):
    """
    Starts the passed instance

    :param str instance_id: a single EC2 instance ID

    :return: None
    """
    ec2 = get_boto_ec2_client()
    ec2.start_instances(InstanceIds=[instance_id])
    log("Starting Instance: " + str(instance_id))


def print_instance_specs():
    """
    Prints the instance specs from the server_instances sequence to assist in debugging/testing.

    :return: None
    """
    for instance in server_instances:
        instance_name = instance["name"]
        instance_region = instance["specs"]["region"]
        instance_id = instance["specs"]["id"]
        log("name:", instance_name, ", region:", instance_region, ", instance id:", instance_id)


def get_instance_specs(name):
    """
    Gets the instance specs from the server_instances sequences based on the passed instance name

    :param str name: The instance name for which specs are to be retrieved. E.g. SERVER2

    :return: A dictionary containing the specs, or None if the passed name is not in the
     server_instances sequence
    """
    try:
        instance_specs = [instance for instance in server_instances if instance["name"] == name][0]
        return instance_specs
    except Exception:
        return None


def unit_test():
    """
    Just a basic unit test capability

    :return: None
    """
    log("Is " + SERVER2_NAME + " running? " +
        ("Yes." if instance_id_is_running(SERVER2_INSTANCEID) else "No."))
    log("Is " + SERVER2_NAME + " running? " +
        ("Yes." if instance_name_is_running(SERVER2_NAME) else "No."))
    stop_instances_by_name(*[SERVER1_NAME, SERVER2_NAME])
    lambda_handler(run_job_action, None)
    lambda_handler(start_server2_action, None)
    lambda_handler(stop_server2_action, None)
    lambda_handler(invalid_action, None)
    log("This is a test")
    log("And another a test", "WARN")


# Tests
#unit_test()
#lambda_handler(stop_all_action, None)
#lambda_handler(run_job_action, None)
#lambda_handler(start_server2_action, None)
