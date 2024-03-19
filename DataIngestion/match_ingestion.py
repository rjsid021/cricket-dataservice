from DataIngestion.query import SELECTED_PLAYER_MAPPER_SQL, GET_PLAYERS_SQL

from DataIngestion.utils.update_player_mapping import PlayerMapping
from common.dao.fetch_db_data import getPandasFactoryDF
from log.log import get_logger

logger = get_logger("Ingestion", "Match Ingestion")


class MatchIngestion:
    def update_player_id(self, ball_by_ball_df, players_existing_mapping):
        ball_by_ball_df['batsman_id'] = ball_by_ball_df['batsman_id'].replace(
            players_existing_mapping).astype(str)
        ball_by_ball_df['non_striker_id'] = ball_by_ball_df['non_striker_id'].replace(
            players_existing_mapping).astype(str)
        ball_by_ball_df['bowler_id'] = ball_by_ball_df['bowler_id'].replace(
            players_existing_mapping).astype(str)
        ball_by_ball_df['out_batsman_id'] = ball_by_ball_df['out_batsman_id'].replace(
            players_existing_mapping).astype(str)
        return ball_by_ball_df

    def match_ingestion(self, ball_by_ball_df, file_path_mapping, players_existing_mapping, squad=None):
        from DataIngestion.config import (
            BAT_CARD_TABLE_NAME,
            BOWL_CARD_TABLE_NAME,
            EXTRAS_TABLE_NAME,
            PARTNERSHIP_TABLE_NAME,
            TEAMS_TABLE_NAME,
            TEAMS_KEY_COL,
            PLAYERS_TABLE_NAME,
            VENUE_TABLE_NAME,
            MATCHES_TABLE_NAME
        )
        from DataIngestion.preprocessor.batting_card import get_cricsheet_batting_card
        from DataIngestion.preprocessor.bowling_card import cricsheet_bowling_card
        from DataIngestion.preprocessor.extras import cricsheet_extras
        from DataIngestion.preprocessor.matches import get_cricsheet_matches
        from DataIngestion.preprocessor.partnership import cricsheet_partnership
        from DataIngestion.preprocessor.players import get_cricsheet_players
        from DataIngestion.preprocessor.teams import get_cricsheet_team_data
        from DataIngestion.preprocessor.ball_summary import get_cricsheet_match_ball_summary
        from DataIngestion.preprocessor.venue import get_cricsheet_venue
        from common.dao.insert_data import insertToDB, upsertDatatoDB
        from common.db_config import DB_NAME
        from DataIngestion.utils.helper import log_cricsheet_files
        from common.dao.fetch_db_data import getMaxId
        from common.dao_client import session
        from DataIngestion.config import FILES_TABLE, FILES_KEY_COL, INNINGS_TABLE_NAME

        # Process Cricsheet data to extract out teams.
        teams_cricsheet_data = get_cricsheet_team_data(ball_by_ball_df)
        if teams_cricsheet_data:
            # Cricsheet Teams data Upsert3
            upsertDatatoDB(session, teams_cricsheet_data[0], DB_NAME, TEAMS_TABLE_NAME, TEAMS_KEY_COL, False)
            # Teams data insert to db3
            insertToDB(session, teams_cricsheet_data[1], DB_NAME, TEAMS_TABLE_NAME, False)
        ball_by_ball_df['batsman_id'] = ball_by_ball_df['batsman_id'].replace(
            players_existing_mapping
        ).astype(str)
        ball_by_ball_df['bowler_id'] = ball_by_ball_df['bowler_id'].replace(
            players_existing_mapping
        ).astype(str)
        ball_by_ball_df['non_striker_id'] = ball_by_ball_df['non_striker_id'].replace(
            players_existing_mapping
        ).astype(str)
        ball_by_ball_df['out_batsman_id'] = ball_by_ball_df['out_batsman_id'].replace(
            players_existing_mapping
        ).astype(str)

        # Process Cricsheet data to extract out Players.
        cricsheet_players_data = get_cricsheet_players(ball_by_ball_df, players_existing_mapping, squad)
        if cricsheet_players_data:
            # Cricsheet players data insert
            insertToDB(session, cricsheet_players_data, DB_NAME, PLAYERS_TABLE_NAME, False)
        players_existing_df = getPandasFactoryDF(session, GET_PLAYERS_SQL)
        players_mapping = dict(zip(players_existing_df['src_player_id'], players_existing_df['player_id']))
        # players_existing_mapping = {**players_existing_mapping, **players_mapping}
        ball_by_ball_df = self.update_player_id(ball_by_ball_df, players_mapping)

        # Process Cricsheet data to extract out Venue.
        cricsheet_venue_data = get_cricsheet_venue(ball_by_ball_df)
        if cricsheet_venue_data:
            # Cricsheet venue data insert
            insertToDB(session, cricsheet_venue_data, DB_NAME, VENUE_TABLE_NAME, False)

        # Process Cricsheet data to extract out Matches info.
        cricsheet_matches_data = get_cricsheet_matches(ball_by_ball_df, players_existing_mapping, squad)
        if cricsheet_matches_data:
            # Cricsheet matches data insert
            insertToDB(session, cricsheet_matches_data, DB_NAME, MATCHES_TABLE_NAME, False)

        # Process Cricsheet data to extract out Batting Card Detail.
        cricsheet_batting_card_data = get_cricsheet_batting_card(ball_by_ball_df)
        if cricsheet_batting_card_data:
            # match batting card insert
            insertToDB(session, cricsheet_batting_card_data, DB_NAME, BAT_CARD_TABLE_NAME, False)

        # Process Cricsheet data to extract out Bowling Card.
        cricsheet_bowling_card_data = cricsheet_bowling_card(ball_by_ball_df)
        if cricsheet_bowling_card_data:
            # match bowling card insert
            insertToDB(session, cricsheet_bowling_card_data, DB_NAME, BOWL_CARD_TABLE_NAME, False)

        # Process Cricsheet data to extract out Extras.
        cricsheet_extras_data = cricsheet_extras(ball_by_ball_df)
        if cricsheet_extras_data:
            # match extras insert
            insertToDB(session, cricsheet_extras_data, DB_NAME, EXTRAS_TABLE_NAME, False)

        # Process Cricsheet data to extract out Partnership.
        cricsheet_partnership_data = cricsheet_partnership(ball_by_ball_df)
        if cricsheet_partnership_data:
            # match partnership insert
            insertToDB(session, cricsheet_partnership_data, DB_NAME, PARTNERSHIP_TABLE_NAME, False)

        # Process Cricsheet data to extract out match ball summary.
        cricsheet_match_ball_summary = get_cricsheet_match_ball_summary(ball_by_ball_df)
        if cricsheet_match_ball_summary:
            # match partnership insert
            insertToDB(session, cricsheet_match_ball_summary, DB_NAME, INNINGS_TABLE_NAME, False)

        # inserting processed squad data filenames into DB
        insertToDB(
            session,
            log_cricsheet_files(
                FILES_KEY_COL,
                file_path_mapping,
                getMaxId(session, FILES_TABLE, FILES_KEY_COL, DB_NAME, False)
            ),
            DB_NAME,
            FILES_TABLE,
            False
        )


class Ingestion:
    def ingestion(self):
        logger.info("Match Core Table Ingestion Invoked")
        from DataIngestion.sources.cricsheet.cricsheet import Cricsheet
        from DataIngestion.sources.nvplay.nvplay import NVPlay
        from DataIngestion.sources.sports_mechanics.sports_mechanics import SportsMechanics
        from common.dao.fetch_db_data import getPandasFactoryDF
        from common.dao_client import session
        players_existing_df = getPandasFactoryDF(session, SELECTED_PLAYER_MAPPER_SQL)
        players_existing_mapping_cricsheet = dict(zip(players_existing_df['cricsheet_id'], players_existing_df['id']))
        players_existing_mapping_nvplay = dict(zip(players_existing_df['nv_play_id'], players_existing_df['id']))
        players_existing_mapping_cricinfo = dict(zip(players_existing_df['cricinfo_id'], players_existing_df['id']))
        players_existing_mapping = {
            **players_existing_mapping_cricsheet,
            **players_existing_mapping_nvplay,
            **players_existing_mapping_cricinfo
        }

        # # Invoke Cricsheet Ingestion
        Cricsheet().match_processing(players_existing_mapping)
        # # Invoke NVPlay Ingestion
        NVPlay().match_processing(players_existing_mapping)
        # update player mapping table if required.
        try:
            PlayerMapping().update_mapping()
        except Exception as e:
            logger.error(e)
        # # Invoke Sports mechanics Ingestion
        SportsMechanics().match_processing()
        logger.info("Match Core Table Ingestion Completed")
