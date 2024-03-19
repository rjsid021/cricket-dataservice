from pathlib import Path
import json
import requests
import os
import pytest
import sys


def requestsResponse(req_type, base_url, api_name, headers, req_body=None):
    # response_code = requests.post(base_url + api_name, headers, data=req_body)
    response = requests.request(req_type, url=base_url+api_name, headers=headers, data=req_body)
    # response_result = (json.dumps(response.json(), indent=4))
    response_result = response.json()
    response_code = response.status_code
    # return [response_code.status_code, response_result]
    return [response_result, response_code]


host_url = "http://10.160.137.233:32104/"
header = {"Content-Type": "application/json"}


def payload():
    return {
      "season": 2020,
      "team_id": 1,
      "overs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    }


def test_highlightStatsCard():
    payload().pop("overs")
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "highlightStatsCard", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_latestPerformances():
    payload().pop("overs")
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "latestPerformances", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_playerProfile():
    payload_body = json.dumps({"player_id": 1})
    api_response = requestsResponse("POST", host_url, "playerProfile", header, payload_body)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)

    payload_body1 = json.dumps({"player_name": "Ro"})
    api_response1 = requestsResponse("POST", host_url, "playerProfile", header, payload_body1)
    assert api_response1[1] == 200
    assert isinstance(api_response1[0], list)


def test_playerSeasonStats():
    payload().pop("overs")
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "playerSeasonStats", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_bestPartnerships():
    payload().pop("overs")
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "bestPartnerships", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_overwiseBowlingStats():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "overwiseBowlingStats", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_overwiseStrikeRate():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "overwiseStrikeRate", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_overwiseBowlingOrder():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "overwiseBowlingOrder", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_positionWiseAvgRuns():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "positionWiseAvgRuns", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_positionWiseBowler():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "positionWiseBowler", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_positionWiseTeamsPerOver():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "positionWiseTeamsPerOver", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_matchPlayingXI():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "matchPlayingXI", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_overWiseStats():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "overWiseStats", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_batsmanVSbowlerStats():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "batsmanVSbowlerStats", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_battingVSbowlerType():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "battingVSbowlerType", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_strikeRateVSdismissals():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "strikeRateVSdismissals", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_highestIndividualScores():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "highestIndividualScores", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], dict)


def test_seasonWiseBattingStats():
    req_payload = json.dumps(payload())
    api_response = requestsResponse("POST", host_url, "seasonWiseBattingStats", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_overSlabWiseRunRate():
    slabs = {"slab1": [1, 2, 3, 4], "slab2": [6, 7, 8, 9], "slab3": [15, 16, 17, 18, 19, 20]}
    req_payload = payload()
    req_payload.update(slabs)
    req_payload = json.dumps(req_payload)
    api_response = requestsResponse("POST", host_url, "overSlabWiseRunRate", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)


def test_batsmanAvgRankingRuns():
    req_payload = json.dumps(req_payload)
    api_response = requestsResponse("POST", host_url, "overSlabWiseRunRate", header, req_payload)
    assert api_response[1] == 200
    assert isinstance(api_response[0], list)