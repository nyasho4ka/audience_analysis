from typing import Optional

from models import UserInfo, GroupInfo

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

    def _get_groups_info(self) -> list:  # TODO IMPLEMENT
        # return self._vk_api.groups.get(user_id=self._user_id, offset=0)
        return [None] * len(self._user_ids)

    def _get_audio_info(self) -> list:  # TODO IMPLEMENT
        #         try:
        #             return self._vk_audio_api.get(owner_id=self._user_id)
        #         except Exception as e:
        #             print(f"Execption occured while getting audio -> {e}")
        #             return {}
        return [None] * len(self._user_ids)


class VkGroupInfoScrapper(VkInfoScrapper):
    def __init__(self, *args):
        super().__init__(*args)
        self._group_id: int = None

    def get_info(self, group_id: int, is_full: bool, fields: list, offset: int) -> 'GroupInfo':
        self._group_id = group_id
        group_info = GroupInfo(
            general_info=self._get_general_info(is_full=is_full, fields=fields),
            discussions_info=self._get_discussions_info(),
            members_info=self._get_members_info(offset=offset),
        )
        return group_info

    def _get_general_info(self, is_full: bool, fields: list) -> dict:
        pass

    def _get_discussions_info(self):
        pass

    def _get_members_info(self, offset: int) -> dict:
        return self._vk_api.groups.getMembers(group_id=self._group_id, offset=offset)
