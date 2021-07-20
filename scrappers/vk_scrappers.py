import pandas as pd
import numpy as np
from typing import Optional

from models import (
    UserInfo, GroupInfo, GeneralGroupInfo,
    DiscussionsGroupInfo, MembersGroupInfo,
)

USER_INFO_FIELDS = [
    'photo_id',
    'verified',
    'sex',
    'bdate',
    'city',
    'country',
    'home_town',
    'has_photo',
    'online',
    'domain',
    'has_mobile',
    'contacts',
    'site',
    'education',
    'universities',
    'schools',
    'status',
    'last_seen',
    'followers_count',
    'occupation',
    'nickname',
    'relatives',
    'relation',
    'personal',
    'connections',
    'exports',
    'activities',
    'interests',
    'music',
    'movies',
    'tv',
    'books',
    'games',
    'about',
    'quotes',
    'is_favorite',
    'is_hidden_from_feed',
    'timezone',
    'screen_name',
    'maiden_name',
    'is_friend',
    'friend_status',
    'career',
    'military',
]

GROUP_INFO_FIELDS = [
    'city',
    'country',
    'place',
    'description',
    'wiki_page',
    'market',
    'members_count',
    'counters',
    'start_date',
    'finish_date',
    'can_post',
    'can_see_all_posts',
    'activity',
    'status',
    'contacts',
    'links',
    'fixed_post',
    'verified',
    'site',
    'ban_info',
    'cover'
]


class VkInfoScrapper(object):
    def __init__(self, vk_api_, vk_audio_api_):
        self._vk_api = vk_api_
        self._vk_audio_api = vk_audio_api_


class VkUserInfoScrapper(VkInfoScrapper):
    def __init__(self, *args):
        super().__init__(*args)
        self._user_ids: Optional[list] = None

    def get_info(self, user_ids: list, is_full: bool, fields: list) -> list:
        self._user_ids = user_ids
        general_users_info: list = self._get_general_info(is_full=is_full, fields=fields)
        groups_users_info: list = self._get_groups_info()
        audio_users_info: list = self._get_audio_info()
        users_info = []
        for general_info, groups_info, audio_info in zip(general_users_info, groups_users_info, audio_users_info):
            user_info = UserInfo(
                general_info=general_info,
                groups_info=groups_info,
                audio_info=audio_info,
            )
            users_info.append(user_info)
        return users_info

    def _get_general_info(self, is_full: bool, fields: list) -> list:
        if bool(is_full) == bool(fields):
            raise AttributeError("You must use or 'is_full' either 'fields' param, not both")

        if is_full:
            requested_fields = USER_INFO_FIELDS
        else:
            requested_fields = fields

        return self._vk_api.users.get(user_ids=self._user_ids, fields=requested_fields)

    def _get_groups_info(self) -> list:
        users_groups_info = []
        requested_fields = ('members_count', 'activity', 'age_limits', 'city')
        headers = ('id', 'name', 'screen_name', 'is_closed', 'type', 'members_count', 'activity', 'age_limits', 'city')
        for user_id in self._user_ids:
            groups_count = self._vk_api.groups.get(user_id=user_id, count=1)['count']
            offsets = [1000 * i for i in range(groups_count // 1000 + 1)]

            user_group_info = []

            for offset in offsets:
                group_info = self._vk_api.groups.get(
                    user_id=user_id, offset=offset, extended=1, fields=requested_fields
                )
                user_group_info.extend(group_info)
            df = pd.DataFrame(np.array(user_group_info), columns=headers)
            users_groups_info.append(df.shape)
            df.to_csv(f'group_data/{user_id}_group.csv')

        return users_groups_info

    def _get_audio_info(self) -> list:
        users_audio_info = []
        headers = ('id', 'artist', 'title', 'duration', 'owner_id')
        for user_id in self._user_ids:
            try:
                user_tracks = self._vk_audio_api.get(owner_id=user_id)
            except Exception as e:
                print(f"Execption occured while getting audio -> {e}")
                users_audio_info.append([])
            else:
                table = []
                for user_track in user_tracks:
                    table.append([
                        user_track['id'],
                        user_track['artist'],
                        user_track['title'],
                        user_track['duration'],
                        user_track['owner_id'],
                    ])
                df = pd.DataFrame(np.array(table), columns=headers)
                users_audio_info.append(df.shape[0])
                df.to_csv(f'audio_data/{user_id}_audio.csv')

        return users_audio_info


class VkGroupInfoScrapper(VkInfoScrapper):
    def __init__(self, *args, group_id: Optional[str] = None):
        super().__init__(*args)
        self._group_id: Optional[str] = group_id
        self._current_group: Optional[GroupInfo] = None
        self._set_current_group()

    def set_group_by_id(self, group_id: str):
        self._group_id = group_id
        self._set_current_group()

    def _set_current_group(self):
        if not self._group_id:
            return
        self._current_group = GroupInfo()

    def get_info(self, is_full: bool, fields: list, offset: int) -> GroupInfo:
        if not self._group_id:
            raise AttributeError("Group Id is not defined for the group scrapper")

        self.get_general_info(is_full=is_full, fields=fields)
        self.get_discussions_info()
        self.get_members_info(offset=offset)
        return self._current_group

    def get_general_info(self, is_full: bool, fields: list) -> GeneralGroupInfo:
        if not self._group_id:
            raise AttributeError("Group Id is not defined for the group scrapper")

        if not self._current_group.general_info:
            if bool(is_full) and bool(fields):
                raise AttributeError("You should choose one of the params (is_full, fields) not both")

            if is_full:
                requested_fields = GROUP_INFO_FIELDS
            else:
                requested_fields = fields

            self._current_group.general_info = GeneralGroupInfo(
                self._vk_api.groups.getById(group_id=self._group_id, fields=requested_fields)
            )

        return self._current_group.general_info

    def get_discussions_info(self) -> DiscussionsGroupInfo:
        if not self._group_id:
            raise AttributeError("Group Id is not defined for the group scrapper")

        if not self._current_group.discussions_info:
            self._current_group.discussions_info = DiscussionsGroupInfo()

        return self._current_group.discussions_info

    def get_members_info(self, offset: int) -> MembersGroupInfo:
        if not self._group_id:
            raise AttributeError("Group Id is not defined for the group scrapper")

        if not self._current_group.members_info:
            self._current_group.members_info = MembersGroupInfo(
                self._vk_api.groups.getMembers(group_id=self._group_id, offset=offset)
            )

        return self._current_group.members_info
