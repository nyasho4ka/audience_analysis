from typing import Optional


class UserInfo(object):
    def __init__(self, general_info: dict, groups_info: dict, audio_info: dict):
        self._parse_general_info(general_info)
        self._parse_groups_info(groups_info)
        self._parse_audio_info(audio_info)

    def _parse_general_info(self, general_info):
        for field, value in general_info.items():
            setattr(self, field, value)

    def _parse_groups_info(self, groups_info):
        pass

    def _parse_audio_info(self, audio_info):
        self.audios = audio_info


class GeneralGroupInfo(object):
    def __init__(self, general_group_info: dict = None):
        self.general_group_info = general_group_info


class DiscussionsGroupInfo(object):
    pass


class MembersGroupInfo(object):
    def __init__(self, members_info: dict = None):
        if members_info:
            self.members_count = members_info['count']
            self.user_ids = members_info['items']
        else:
            self.members_count = 0
            self.user_ids = None


class GroupInfo(object):
    def __init__(
        self,
        general_info: Optional[GeneralGroupInfo] = None,
        discussions_info: Optional[DiscussionsGroupInfo] = None,
        members_info: Optional[MembersGroupInfo] = None,
    ):
        self.general_info = general_info
        self.discussions_info = discussions_info
        self.members_info = members_info
