# Takes argument among qa/dev/prod/replica & excel file and registers the users. Allowlist is not modified.
# Usage: python kw_bulk_user_upload.py qa /Users/username/Desktop/bulk_user_upload.xlsx

import argparse
import os
import sys

import pandas as pd
import requests
from pandas import DataFrame


def fixCode(code):
    if code is None:
        return None
    code = str(code)
    code.replace(" ", "")
    code.replace("+", "")
    code.replace("-", "")
    try:
        code = int(code.split('.')[0])
        if (int)(code) == 91:
            return "+91"
        return "+" + code
    except ValueError as e:
        print(e)
        print("Couldn't fix code for {}".format(code))
        return None


# parse excel data to userRequestBody


def excelDataToRequestPayloads(data: DataFrame):
    if (data is None or data.empty):
        print("No data found in the excel file")
        return None
    data = data.astype(str)
    # print(data)
    # print(data.head())
    payloads = []

    def transformRow(row):
        if (row is None or row.empty):
            return
        user: userRequestBody = userRequestBody()
        user.tenant_name = "jiosports"
        countryCode = fixCode(row["Country Code"])
        if countryCode is None:
            print("Skipping Couldn' fix code for row {}".format(row))
            return

        email: str = row["Email"]
        email = email.strip() if email != None and "@" in email else None
        if countryCode == "+91":
            user.contacts.append(countryCode + row["Phone"])
            if email != None:
                user.contacts.append(email)
        else:
            if email is None:
                print("Skipping Email is empty for row {}".format(row))
                return
            user.contacts.append(email)
            user.metadata["number"] = countryCode + row["Phone"]

        user.metadata["name"] = row["Name"]
        user.role = row["Role"].lower()
        print(user.to_payload())
        payloads.append(user.to_payload())

    data.apply(transformRow, axis=1)
    # print(payloads)
    return payloads


# call register endpoint
def callRegisterEndpoint(envURL, payloads):
    if envURL is None:
        print("Invalid environment URL")
        return
    # make post request to register endpoint and check for failure reponse
    for payload in payloads:
        response = requests.post(
            envURL + "/v2/register", json=payload, timeout=5)
        response = response.json()
        if response["status"] == "SUCCESS":
            continue
        else:

            if response["reason"]["code"] == "ASE-1012":
                continue
            print("Failed to register user: " + str(payload) +
                  " for reason " + response["reason"]["reason"])
            # print(response)
        break
    return


class userRequestBody:
    tenant_name = "jiosports"
    metadata = {}
    role = ""
    contacts = []

    def __init__(self):
        self.metadata = {}
        self.contacts = []
        self.role = ""
        self.tenant_name = "jiosports"

    def to_payload(self):
        return {
            "sub": self.tenant_name,
            "roles": [self.role],
            "contactTokens": [{"contact": contact, "cvToken": ""} for contact in self.contacts],
            "metadata": self.metadata,
        }


if __name__ == "__main__":

    # parse arguemnts
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "env", help="environment (qa/dev/prod/replica)"
    )
    parser.add_argument("file", help="excel file path")
    args = parser.parse_args()

    if args.env not in ["qa", "dev"]:
        print("Invalid environment: " + args.env + " (qa/dev)")
        sys.exit(1)
    file: str = args.file
    if not os.path.isfile(file):
        print(f"Invalid file path: {file}")
        sys.exit(1)

    # get excel data
    payloads = excelDataToRequestPayloads(pd.read_excel(file, engine='openpyxl', sheet_name="Production"))
    if payloads is None or len(payloads) == 0:
        print("No user found in the excel file")
        sys.exit(0)
    print("Found {} users".format(len(payloads)))

    envURL = None
    if args.env.lower() == "qa":
        envURL = "http://10.168.134.226:32087"
    elif args.env.lower() == "dev":
        envURL = "http://10.168.134.226:32086"

    callRegisterEndpoint(envURL, payloads)
