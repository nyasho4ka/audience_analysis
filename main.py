import argparse
import time
from multiprocessing import Process
import pandas as pd
import numpy as np
from processor import VkProcessor
from utils import get_phone_and_password, get_required_table_fields


def worker(vk_processor_, group_id_, offset_, user_fields_):
    group_info = vk_processor_.get_group_info(group_id=group_id_, offset=offset_)

    user_data = []
    users_info = vk_processor_.get_user_info(group_info.user_ids, is_full=True)

    for user_info in users_info:
        user_list = []
        for field in user_fields_:
            if hasattr(user_info, field):
                user_list.append(getattr(user_info, field))
            else:
                user_list.append('nan')

        user_data.append(user_list)

    pd.DataFrame(np.array(user_data), columns=user_fields_).to_csv(f'result_{offset_}_{offset_ + 1000}.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--group_id', dest='group_id', type=str, required=True,
    )
    args = parser.parse_args()

    vk_processor = VkProcessor(*get_phone_and_password())

    final_result = []
    user_fields = get_required_table_fields()
    start_time = time.time()
    group_id = args.group_id
    threads = []
    for offset in range(0, 10001, 1000):
        threads.append(Process(target=worker, args=(vk_processor, group_id, offset, user_fields)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    print(f"TIME HAS PASSED: {end_time - start_time} s.")
