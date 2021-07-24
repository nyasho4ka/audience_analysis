import argparse
import time
import os
from multiprocessing import Pool

import pandas as pd
import numpy as np
from models import GroupInfo, MembersGroupInfo
from processor import VkProcessor
from utils import get_phone_and_password, get_required_table_fields


def worker(group_id_, offset_, user_fields_):
    worker_id = os.getpid()

    vk_processor_ = VkProcessor(*get_phone_and_password())
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

    pd.DataFrame(np.array(user_data), columns=user_fields_).to_csv(f'result_{offset_}_{offset_ + 1000}.csv')

    print(f'WORKER_ID={worker_id} - FINISH')
    return 'ok'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--group_id', dest='group_id', type=str, required=True,
    )
    args = parser.parse_args()

    final_result = []
    user_fields = get_required_table_fields()
    start_time = time.time()
    group_id = args.group_id

    with Pool(processes=4) as pool:
        async_results = []
        for offset in range(0, 10000, 1000):
            async_result = pool.apply_async(
                func=worker,
                args=(group_id, offset, user_fields)
            )
            async_results.append(async_result)
            time.sleep(5)
            print(f"offset = {offset}")

        for async_result in async_results:
            async_result.wait()

    end_time = time.time()
    print(f"TIME HAS PASSED: {end_time - start_time} s.")
