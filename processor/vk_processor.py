import vk_api
from vk_api.audio import VkAudio

from scrappers import VkGroupInfoScrapper, VkUserInfoScrapper


class VkProcessor(object):
    def __init__(self, username, password):
        self._vk_session = self._get_authed_vk_session(username, password)
        self._vk_api = self._vk_session.get_api()
        self._vk_audio_api = VkAudio(vk=self._vk_session)
        self._vk_user_info_scrapper = VkUserInfoScrapper(
            self._vk_api, self._vk_audio_api,
        )
        self._vk_group_info_scrapper = VkGroupInfoScrapper(
            self._vk_api, self._vk_audio_api,
        )

    @staticmethod
    def _get_authed_vk_session(username, password):
        vk_session = vk_api.VkApi(username, password)
        vk_session.auth()
        return vk_session

    def get_user_info(self, user_ids: list, is_full: bool = None, fields: list = None) -> 'UserInfo':
        users_info = self._vk_user_info_scrapper.get_info(user_ids, is_full=is_full, fields=fields)
        return users_info

    def get_users_info(self, user_ids: list, is_full: bool = None, fields: list = None) -> list:
        users_info = self._vk_user_info_scrapper.get_info(user_ids, is_full=is_full, fields=fields)
        return users_info

    def get_group_info(self, is_full: bool = None, fields: list = None, offset: int = 0) -> 'GroupInfo':
        if is_full and fields:
            raise AttributeError("You can use is_full or fields param not both")
        return self._vk_group_info_scrapper.get_info(is_full=is_full, fields=fields, offset=offset)
    #
    # def get_groups_info(self, group_ids: list, is_full: bool = None, fields: list = None) -> list:
    #     groups_info = []
    #     for group_id in group_ids:
    #         groups_info.append(self.get_group_info(group_id, is_full=is_full, fields=fields))
    #     return groups_info
