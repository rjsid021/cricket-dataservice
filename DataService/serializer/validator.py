import datetime
import base64
from marshmallow import Schema, fields, ValidationError
import re


class StrField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, list):
            return value
        elif isinstance(value, str) & (value != '' and all(
                chr.isalpha() or chr.isspace() or chr in ["-", ";", ".", "_"] for chr in value)):
            return value
        else:
            raise ValidationError('Field should be str or list')


class VarcharField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, list):
            return value
        elif isinstance(value, str):
            if value != "":
                if re.match("^[a-zA-Z0-9\s]*$", value):
                    return value
                else:
                    raise ValidationError('Field should be alphanumeric string or list')
            elif value == "":
                return value
            else:
                raise ValidationError('Field should be alphanumeric string or list')
        else:
            raise ValidationError('Field should be alphanumeric string or list')


class StrBoolField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if value.lower() in ["yes", "no"]:
            return value
        else:
            raise ValidationError('Field should be either Yes or No')


class StrDateField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, list):
            return value
        elif isinstance(value, str) and (value != '' and all(
                chr.isalnum() or chr.isspace() or chr in [",", "/", "-"] for chr in value)):
            try:
                # Attempt to parse the value as '2023-03-17'
                return datetime.datetime.strptime(value, '%Y-%m-%d').date()
            except ValueError:
                try:
                    # Attempt to parse the value as 'Saturday, Mar 02, 2024'
                    return datetime.datetime.strptime(value, '%A, %b %d, %Y').date()
                except ValueError:
                    raise ValidationError(
                        'Invalid date format. Supported formats are "2023-03-17" and "Saturday, Mar 02, 2024"')
        else:
            raise ValidationError('Field should be str or list')


class ImageField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        try:
            if value == "":
                return value
            elif (value != "") & (isinstance(value, str)):
                image = base64.b64decode(value)
                return value
            else:
                raise ValidationError("Invalid base64 encoded image data")
        except TypeError:
            raise ValidationError("Invalid base64 encoded image data")


class IntField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, int) or isinstance(value, list):
            return value
        else:
            raise ValidationError('Field should be int or list')


class GlobalFilters(Schema):
    venue = IntField()
    player_id = IntField()
    team_id = IntField()
    bowling_type = StrField()
    batting_type = StrField()
    innings = IntField()
    winning_type = StrField()
    season = IntField()
    competition_name = VarcharField()
    year = IntField()
    phase = IntField()
    bowler_id = IntField()
    batsman_id = IntField()
    bowler = StrField()
    batsman = StrField()
    striker_name = StrField()
    non_striker_name = StrField()
    striker = IntField()
    non_striker = IntField()
    player_name = StrField()
    user_name = StrField()
    pitch_type = StrField()
    position = StrField()
    player_type = StrField()
    retained = StrField()
    speciality = StrField()
    leagues = StrField()
    in_auction = StrField()
    overs = IntField()
    match_id = IntField()
    team1 = IntField()
    team2 = IntField()
    slab1 = IntField()
    slab2 = IntField()
    slab3 = IntField()
    slab4 = IntField()
    bowler_team_id = IntField()
    batsman_team_id = IntField()
    comparison_type = StrField()
    match_date = fields.Date()
    data = StrField()
    match_phase = VarcharField()
    stat_type = StrField()
    min_innings = fields.Int()
    include_batsman = StrField()
    best_performing = StrField()
    worst_performing = StrField()
    playing_xi = StrField()
    home_team = StrField()
    start_over = fields.Int()
    perf_sort = StrField()
    max_rows = fields.Int()
    upper = fields.Int()
    lower = fields.Int()
    total_matches_gt = fields.Int()
    record_date = StrDateField()
    to_date = StrDateField()
    from_date = StrDateField()
    agg_key = fields.Str()
    agg_type = fields.Str()
    over_no = IntField()
    player_skill = StrField()
    sort_key = fields.Str()
    page = fields.Int()
    record_count = fields.Int()
    asc = fields.Bool()
    is_won = fields.Int()
    team = StrField()
    team_name = VarcharField()
    user_team_name = StrField()
    replaced_over_no = fields.Int()
    replaced_player = StrField()
    impact_player = StrField()
    total_runs = fields.Int()
    total_balls = fields.Int()
    match_type = VarcharField()
    sort_key_bowling = fields.Str()
    entry_point = StrField()
    pressure_cat = StrField()
    batting_position = IntField()
    auction = fields.Str()
    availability_status = StrField()
    app = fields.Str()
    user_id = fields.Int()
    month = IntField()
    day = IntField()

    start_time = fields.DateTime()
    completion_time = fields.DateTime()
    form_filler = StrField()
    fatigue_level_rating = fields.Int()
    sleep_rating = fields.Int()
    muscle_soreness_rating = fields.Int()
    stress_levels_rating = fields.Int()
    wellness_rating = fields.Int()
    trained_today = StrBoolField()
    played_today = StrBoolField()
    reason_noplay_or_train = StrField()
    batting_train_mins = fields.Int()
    batting_train_rpe = fields.Int()
    bowling_train_mins = fields.Int()
    bowling_train_rpe = fields.Int()
    bowling_train_balls = fields.Int()
    fielding_train_mins = fields.Int()
    fielding_train_rpe = fields.Int()
    strength_mins = fields.Int()
    strength_rpe = fields.Int()
    running_mins = fields.Int()
    running_rpe = fields.Int()
    cross_training_mins = fields.Int()
    cross_training_rpe = fields.Int()
    rehab_mins = fields.Int()
    rehab_rpe = fields.Int()
    batting_match_mins = fields.Int()
    batting_match_rpe = fields.Int()
    bowling_match_mins = fields.Int()
    bowling_match_rpe = fields.Int()
    bowling_match_balls = fields.Int()
    fielding_match_mins = fields.Int()
    fielding_match_rpe = fields.Int()
    id = fields.Int()
    match_peak_load = fields.Decimal()
    train_balls = fields.Int()
    match_balls = fields.Int()
    uuid = VarcharField()
    title = StrField()
    module_name = VarcharField()
    description = VarcharField()
    resolution = VarcharField()
    state = StrField()
    update_ts = fields.DateTime()
    resolution_image = ImageField()
    desc_file_name = StrField()
    desc_image = ImageField()
    s_no = fields.Int()
    module = VarcharField()
    start_date = StrDateField()
    end_date = StrDateField()
    player_list = StrField()
    back_up = fields.Int()
    caught_and_bowled = fields.Int()
    clean_takes = fields.Int()
    direct_hit = fields.Int()
    dives_made = fields.Int()
    dives_missed = fields.Int()
    dropped_percent_difficulty = fields.Int()
    good_attempt = fields.Int()
    good_return = fields.Int()
    miss_fields = fields.Int()
    miss_fields_cost = fields.Int()
    missed_runs = fields.Int()
    missed_shy = fields.Int()
    poor_return = fields.Int()
    pop_ups = fields.Int()
    returns_taken_plus = fields.Int()
    returns_untidy = fields.Int()
    run_out_obtained = fields.Int()
    runs_saved = fields.Int()
    standing_back_minus = fields.Int()
    standing_back_plus = fields.Int()
    standing_up_minus = fields.Int()
    standing_up_plus = fields.Int()
    stumping = fields.Int()
    support_run = fields.Int()
    taken = fields.Int()
    match_name = fields.Str()
    app_type = fields.Str()

    class Meta:
        strict = True
