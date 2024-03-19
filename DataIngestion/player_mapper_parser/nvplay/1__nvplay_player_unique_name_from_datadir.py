import pandas as pd

from DataIngestion.utils.helper import readCSV

if __name__ == "__main__":
    # Specify the path to the folder you want to list
    ilt = '/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/ILT20.csv'
    mlc = '/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/MLC.csv'
    wpl = '/Users/achintya.chaudhary/Documents/projects/CricketDataService/DataIngestion/player_mapper_parser/nvplay/WPL.csv'

    ilt = readCSV(ilt)
    mlc = readCSV(mlc)
    wpl = readCSV(wpl)

    ilt = ilt[['Batter', 'Bowler', 'Non-Striker']]
    mlc = mlc[['Batter', 'Bowler', 'Non-Striker']]
    wpl = wpl[['Batter', 'Bowler', 'Non-Striker']]

    merged = pd.concat([ilt, mlc, wpl])
    # Create a new DataFrame with a single column
    new_df = pd.DataFrame()

    # Concatenate the data from the three columns into the new column
    new_df['player'] = pd.concat(
        [merged['Batter'].astype(str), merged['Bowler'].astype(str), merged['Non-Striker'].astype(str)]
    )
    new_df = new_df.drop_duplicates()
    new_df.to_csv("ukt.csv")
