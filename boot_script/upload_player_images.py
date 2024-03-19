from azure.storage.blob import BlockBlobService

from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session
from common.db_config import DB_NAME
from common.utils.helper import getEnvVariables

dir_name = "sports"
container_name = getEnvVariables('CONTAINER_NAME')
block_blob_service = BlockBlobService(
    account_name=getEnvVariables('STORAGE_ACCOUNT_NAME'),
    account_key=getEnvVariables('STORAGE_ACCOUNT_KEY')
)


def check_player_image(player_name):
    print("--------------->", player_name)
    return block_blob_service.exists(container_name, f"players/{player_name}.png")
    print("------------------------------------")


if __name__ == "__main__":
    # BLOB Image Folder Download
    STORAGE_ACCOUNT_NAME = 'jioaishared'
    STORAGE_ACCOUNT_KEY = 'OYRIjKqneeiI+UrDyLHV7CBOudfdxb73AqkdtUYX3UGYVynUDjQdCtv60a73oEf++qIYnY6844QUOSPrcndLuQ=='

    blobs = block_blob_service.list_blobs(container_name, prefix='players')

    player_mapping = getPandasFactoryDF(session, f"SELECT * FROM {DB_NAME}.playermapping")
    player_mapping = player_mapping[['name']]
    player_mapping = player_mapping.drop_duplicates(subset=['name'])

    # iterate through all the rows from above dataframe
    for index, row in player_mapping.iterrows():
        # check if the image exists in the blob storage
        # make 2 spaces to 1 space
        import re


        def remove_extra_spaces(input_string):
            # Use a regular expression to replace multiple spaces with a single space
            cleaned_string = re.sub(r'\s+', ' ', input_string)
            return cleaned_string

        print("-------------> index : ",index)
        name = remove_extra_spaces(row['name']).strip()
        name = name.replace("  ", " ")
        name = name.lower().replace(" ", "-").replace("'", "").replace("é", 'e')
        name = name.replace("ö", "o").replace("ç", "c").replace("ñ", "n").replace("ü", "u").replace("à", "a")

        if not check_player_image(name):
            print(f"uploading {name} to blob storage @@@@@")
            # if player image not found upload to azure blob storage
            block_blob_service.create_blob_from_path(
                container_name,
                f"players/{name}.png",
                f"/Users/achintya.chaudhary/Documents/projects/CricketDataService/boot_script/placeholder.png"
            )
