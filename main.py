import time
import os
from multiprocessing import Pool

import pandas as pd
import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from models import MembersGroupInfo
from processor import VkProcessor
from utils import (
    get_vk_credentials,
    get_spotify_credentials,
    get_required_table_fields,
    get_args, timer)


def group_process_worker(group_id_, offset_, user_fields_):
    worker_id = os.getpid()

    vk_processor_ = VkProcessor(*get_vk_credentials())
    vk_processor_.set_group_by_id(group_id=group_id_)

    members_group_info: MembersGroupInfo = vk_processor_.get_members_group_info(offset=offset_)

    user_data = []
    user_ids = members_group_info.user_ids
    users_info = vk_processor_.get_users_info(user_ids, is_full=True)

    for user_info in users_info:
        user_list = []
        for field in user_fields_:
            if hasattr(user_info, field):
                user_list.append(getattr(user_info, field))
            else:
                user_list.append('nan')

        user_data.append(user_list)

    pd.DataFrame(np.array(user_data), columns=user_fields_).to_csv(f'result_data/result_{offset_}_{offset_ + 1000}.csv')

    print(f'WORKER_ID={worker_id} - FINISH')
    return 0


def process_group(group_id: str, user_fields: list):
    with Pool(processes=4) as pool:
        async_results = []
        for offset in range(0, 10000, 1000):  # TODO make number of people configurable
            async_result = pool.apply_async(
                func=group_process_worker,
                args=(group_id, offset, user_fields)
            )
            async_results.append(async_result)
            time.sleep(5)
            print(f"offset = {offset}")

        for async_result in async_results:
            async_result.wait()


def audio_process_worker(audio_files):
    """
    Several audio files are not big enough to lead to memory leaks,
    So we can just process them in parallel
    :return:
    """
    audio_dfs = pd.concat((pd.read_csv(f'audio_data/{audio_file}' for audio_file in audio_files)))
    return audio_dfs['artist'].str.split(', | feat. ', expand=True, n=1)[0].unique()


def get_all_unique_artists():
    audio_files = (filename for filename in os.listdir('audio_data') if '.csv' in filename)
    audio_chunks = np.array_split(audio_files, os.cpu_count())
    with Pool(processes=os.cpu_count()) as pool:
        async_results = []
        for audio_chunk in audio_chunks:
            async_results.append(pool.apply_async(
                audio_process_worker,
                args=(audio_chunk,)
            ))

        for async_result in async_results:
            async_result.wait()

    return set().union(*async_results)


def get_all_artists_info(all_unique_artists):
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(*get_spotify_credentials()))
    # Get all artists meta info
    # return it and create dataframe
    return []


def process_audio():
    all_unique_artists = get_all_unique_artists()
    all_artists_info = get_all_artists_info(all_unique_artists)
    pd.to_csv(all_artists_info)


@timer
def main():
    args = get_args()
    user_fields = get_required_table_fields()
    group_id = args.group_id

    process_group(group_id, user_fields)
    process_audio()


if __name__ == '__main__':
    main()
