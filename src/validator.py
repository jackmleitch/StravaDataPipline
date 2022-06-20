import sys
import requests
import json
import configparser
from src.utilities.redshift_utils import connect_redshift


def execute_test(db_conn, script_1: str, script_2: str, comp_operator: str) -> bool:
    """
    Execute test made up of two scripts and a comparison operator
    :param comp_operator: comparison operator to compare script outcome
        (equals, greater_equals, greater, less_equals, less, not_equals)
    :return: True/False for test pass/fail
    """
    # execute the 1st script and store the result
    cursor = db_conn.cursor()
    sql_file = open(script_1, "r")
    cursor.execute(sql_file.read())
    record = cursor.fetchone()
    result_1 = record[0]
    db_conn.commit()
    cursor.close()

    # execute the 2nd script and store the result
    cursor = db_conn.cursor()
    sql_file = open(script_2, "r")
    cursor.execute(sql_file.read())
    record = cursor.fetchone()
    result_2 = record[0]
    db_conn.commit()
    cursor.close()

    print("Result 1 = " + str(result_1))
    print("Result 2 = " + str(result_2))

    # compare values based on the comp_operator
    if comp_operator == "equals":
        return result_1 == result_2
    elif comp_operator == "greater_equals":
        return result_1 >= result_2
    elif comp_operator == "greater":
        return result_1 > result_2
    elif comp_operator == "less_equals":
        return result_1 <= result_2
    elif comp_operator == "less":
        return result_1 < result_2
    elif comp_operator == "not_equal":
        return result_1 != result_2

    # tests have failed if we make it here
    return False


# test_result should be True/False
def send_slack_notification(
    webhook_url: str,
    script_1: str,
    script_2: str,
    comp_operator: str,
    test_result: bool,
) -> bool:

    try:
        if test_result == True:
            message = (
                f"Validation Test Passed!: {script_1} / {script_2} / {comp_operator}"
            )
        else:
            message = (
                f"Validation Test FAILED!: {script_1} / {script_2} / {comp_operator}"
            )
        # send test result to Slack
        slack_data = {"text": message}
        response = requests.post(
            webhook_url,
            data=json.dumps(slack_data),
            headers={"Content-Type": "application/json"},
        )
        # if post request to Slack fails
        if response.status_code != 200:
            print(response)
            return False

    except Exception as e:
        print("Error sending slack notification")
        print(str(e))
        return False


if __name__ == "__main__":

    if len(sys.argv) == 2 and sys.argv[1] == "-h":
        print("Usage: python validator.py script1.sql script2.sql comparison_operator")
        print(
            "Valid comparison_operator values: (equals, greater_equals, greater, less_equals, less, not_equals)"
        )
        exit(0)

    if len(sys.argv) != 5:
        print(
            "Usage: python validator.py script1.sql script2.sql comparison_operator severity_level"
        )
        exit(-1)

    # load arguments
    script_1 = sys.argv[1]
    script_2 = sys.argv[2]
    comp_operator = sys.argv[3]
    sev_level = sys.argv[4]
    # execute test
    db_conn = connect_redshift()
    test_result = execute_test(db_conn, script_1, script_2, comp_operator)
    print("Result of test: " + str(test_result))
    # load slack webhook_url
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    webhook_url = parser.get("slack_config", "webhook_url")
    send_slack_notification(webhook_url, script_1, script_2, comp_operator, test_result)
    # exit
    if sev_level == "halt":
        exit(1)
    else:
        exit(0)
