from DataIngestion import load_timestamp
from pprint import pprint
from DataIngestion.config import (
    BAT_CARD_TABLE_NAME,
    BOWL_CARD_TABLE_NAME,
    CONTRIBUTION_CONSTRAINTS_DATA_PATH,
    EXTRAS_TABLE_NAME,
    FILES_KEY_COL,
    FILES_TABLE,
    INNINGS_TABLE_NAME,
    MATCHES_TABLE_NAME,
    PARTNERSHIP_TABLE_NAME,
    PITCH_TYPE_2019_TO_2021_DATA_PATH,
    PITCH_TYPE_DATA_PATH,
    PLAYERS_TABLE_NAME,
    ROOT_DATA_PATH,
    SQUAD_ROOT_PATH,
    TEAMS_KEY_COL,
    TEAMS_TABLE_NAME,
    VENUE_TABLE_NAME, CONTRIBUTION_SCORE_TABLE_NAME, )
from DataIngestion.preprocessor.ball_summary import getMatchBallSummaryData
from DataIngestion.preprocessor.batting_card import getBattingCardData
from DataIngestion.preprocessor.bowling_card import getBowlingCardData
from DataIngestion.preprocessor.contribution_score import get_contribution_score
from DataIngestion.preprocessor.extras import getExtrasData
from DataIngestion.preprocessor.matches import getMatchesData
from DataIngestion.preprocessor.partnership import getPartnershipData
from DataIngestion.preprocessor.players import getPlayersData
from DataIngestion.preprocessor.teams import getTeamsData
from DataIngestion.preprocessor.venue import getVenueData
from DataIngestion.query import (
    GET_EXISTING_FILES
)
from DataIngestion.utils.helper import getFiles, getLatestFiles, logFilesIntoDB
from common.dao.fetch_db_data import getAlreadyExistingValue, getMaxId
from common.dao.insert_data import insertToDB, upsertDatatoDB
from common.dao_client import session
from common.db_config import DB_NAME
from log.log import get_logger


class SportsMechanics:
    def __init__(self):
        # getting list of already loaded files
        self.loaded_files = getAlreadyExistingValue(session, GET_EXISTING_FILES)
        self.loaded_files = [file for file in self.loaded_files]
        self.logger = get_logger("Ingestion", "Ingestion")

    def match_processing(self):
        root_files = getLatestFiles(self.loaded_files, getFiles(ROOT_DATA_PATH))
        root_data_files = root_files if len(root_files) > 1 else {}
        squad_data_files = getLatestFiles(self.loaded_files, getFiles(SQUAD_ROOT_PATH))

        self.logger.info("----------- Root data files ----------")
        pprint(root_data_files, indent=4)
        self.logger.info("----------- Squad data files ----------")
        pprint(squad_data_files, indent=4)

        teams_data = getTeamsData(
            session,
            root_data_files,
            squad_data_files,
            load_timestamp
        )
        if teams_data:
            # teams data upsert
            upsertDatatoDB(session, teams_data[0], DB_NAME, TEAMS_TABLE_NAME, TEAMS_KEY_COL)

            # teams data insert to db
            insertToDB(session, teams_data[1], DB_NAME, TEAMS_TABLE_NAME)

        players_data = getPlayersData(
            session,
            squad_data_files,
            load_timestamp
        )
        if players_data:
            # players data insert
            insertToDB(session, players_data, DB_NAME, PLAYERS_TABLE_NAME)

        venue_data = getVenueData(
            session,
            root_data_files,
            load_timestamp
        )
        if venue_data:
            # venue data insert
            insertToDB(session, venue_data, DB_NAME, VENUE_TABLE_NAME)

        matches_data = getMatchesData(
            session,
            root_data_files,
            squad_data_files,
            PITCH_TYPE_DATA_PATH,
            load_timestamp,
            PITCH_TYPE_2019_TO_2021_DATA_PATH
        )
        if matches_data:
            # matches data insert
            insertToDB(session, matches_data, DB_NAME, MATCHES_TABLE_NAME)

        batting_card_data = getBattingCardData(
            session,
            root_data_files,
            load_timestamp
        )
        if batting_card_data:
            # match batting card insert
            insertToDB(session, batting_card_data, DB_NAME, BAT_CARD_TABLE_NAME)

        bowling_card_data = getBowlingCardData(
            session,
            root_data_files,
            load_timestamp
        )
        if bowling_card_data:
            # match bowling card insert
            insertToDB(session, bowling_card_data, DB_NAME, BOWL_CARD_TABLE_NAME)

        extras_data = getExtrasData(
            session,
            root_data_files,
            load_timestamp
        )
        if extras_data:
            # match extras insert
            insertToDB(session, extras_data, DB_NAME, EXTRAS_TABLE_NAME)

        partnership_data = getPartnershipData(
            session,
            root_data_files,
            load_timestamp
        )
        if partnership_data:
            # match partnership insert
            insertToDB(session, partnership_data, DB_NAME, PARTNERSHIP_TABLE_NAME)

        ball_summary_data = getMatchBallSummaryData(
            session,
            root_data_files,
            load_timestamp
        )
        if ball_summary_data:
            # match ball summary data insert
            insertToDB(session, ball_summary_data, DB_NAME, INNINGS_TABLE_NAME)

        contribution_score = get_contribution_score(
            CONTRIBUTION_CONSTRAINTS_DATA_PATH,
            load_timestamp
        )
        if contribution_score:
            insertToDB(session, contribution_score, DB_NAME, CONTRIBUTION_SCORE_TABLE_NAME)

        # Exclude <file_name>-matchSchedule.js for other matches to be used later
        file_name_deletion = []
        for file_name, file_path in root_data_files.items():
            if file_name.split("-")[1] == "matchSchedule.js":
                file_name_deletion.append(file_name)
        for file in file_name_deletion:
            root_data_files.pop(file, None)

        # inserting processed matches data filenames into DB
        insertToDB(
            session,
            logFilesIntoDB(
                FILES_KEY_COL,
                root_data_files,
                getMaxId(session, FILES_TABLE, FILES_KEY_COL, DB_NAME),
                load_timestamp
            ),
            DB_NAME,
            FILES_TABLE
        )

        # inserting processed squad data filenames into DB
        insertToDB(
            session,
            logFilesIntoDB(
                FILES_KEY_COL,
                squad_data_files,
                getMaxId(session, FILES_TABLE, FILES_KEY_COL, DB_NAME),
                load_timestamp
            ),
            DB_NAME,
            FILES_TABLE
        )
