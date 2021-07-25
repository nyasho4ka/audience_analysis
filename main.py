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
    try:
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
    except Exception as ex:
        print(f"Unexpected exception: {ex}")


def process_group(group_id: str, user_fields: list):
    with Pool(processes=os.cpu_count() if os.cpu_count() else 4) as pool:
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
    final_df = pd.concat([pd.read_csv(f'audio_data/{audio_file}') for audio_file in audio_files])
    return set(final_df['artist'].unique())  # VANILLA VERSION OF GETTING ARTISTS


def get_all_unique_artists_from_dataset():
    audio_files = [filename for filename in os.listdir('audio_data') if '.csv' in filename]
    audio_chunks = np.array_split(audio_files, 4)
    with Pool(processes=os.cpu_count() if os.cpu_count() < 4 else 4) as pool:
        async_results = []
        for i, audio_chunk in enumerate(audio_chunks, 1):
            async_result = pool.apply_async(
                audio_process_worker,
                args=(audio_chunk,)
            )
            async_results.append(async_result)

        return set().union(*[async_result.get() for async_result in async_results])


def get_artists_ids(sp, all_unique_artists):
    artist_ids = set()
    print("Start getting artists' ids")
    # artist_row variable may contain several artists
    # (ex. PORCHY, MAY WAVE$, JEEMBO, LOQIEMEAN, THOMAS MRAZ, TVETH, SOULOUD, MARKUL, OXXXYMIRON)
    unique_artists_number = len(all_unique_artists)
    for i, artist_row in enumerate(all_unique_artists, 1):
        items = sp.search(q=artist_row, limit=1)['tracks']['items']
        if len(items) > 0:
            artists = items[0]['artists']
            for artist in artists:
                artist_ids.add(artist['id'])
        print(f'Progress: {round(i / unique_artists_number * 100, 2)}% complete\r', end='')
        time.sleep(0.01)
    print()
    print("Finish getting artists' ids")
    return artist_ids


def save_artists_ids(artists_ids):
    with open('artists_ids.txt', 'w', encoding='utf-8') as output:
        for artist_id in artists_ids:
            output.write(f'{artist_id}\n')


def get_artists_data_by_id(sp):
    print("Start getting artists' info with SpotifyAPI")
    step = 50

    with open('artists_ids.txt', encoding='utf-8') as inp:
        artists_ids = [line[:-1] for line in inp.readlines()]

    artists_ids_list = list(artists_ids)
    async_results = []
    with Pool(processes=os.cpu_count() if os.cpu_count() < 4 else 4) as pool:
        for i in range(0, len(artists_ids_list), step):
            async_result = pool.apply_async(
                sp.artists,
                args=artists_ids_list[i:i+step]
            )
            async_results.append(async_result)

        for async_result in async_results:
            async_result.wait()
        print("Finish getting artists' info with SpotifyAPI")
        final_result = []
        for async_result in async_results:
            artists_data = async_result.get()
            artist_pre_dataframe = []
            for artist in artists_data['artists']:
                artist_pre_dataframe.append([
                    artist['id'],
                    artist['name'],
                    artist['genres'],
                    artist['popularity'],
                    artist['followers'],
                ])

            final_result.extend(artist_pre_dataframe)
        return final_result


def get_all_artists_info(all_unique_artists):
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(*get_spotify_credentials()))
    artists_ids = get_artists_ids(sp, all_unique_artists)
    save_artists_ids(artists_ids)
    artists_data = get_artists_data_by_id(sp)
    headers = ('id', 'name', 'genres', 'popularity', 'followers')
    return pd.DataFrame(artists_data, columns=headers)


def process_audio():
    all_unique_artists = get_all_unique_artists_from_dataset()
    all_artists_info = get_all_artists_info(all_unique_artists)
    all_artists_info.to_csv('full_artists_df.csv')


@timer
def main():
    args = get_args()
    user_fields = get_required_table_fields()
    group_id = args.group_id

    process_group(group_id, user_fields)
    process_audio()


if __name__ == '__main__':
    main()
