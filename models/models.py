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
        pass


class GroupInfo(object):
    def __init__(
        self,
        general_info: dict,
        discussions_info,
        members_info: dict,
    ):
        self.members_count = 0
        self.user_ids = None
        self._parse_general_info(general_info)
        self._parse_discussions_info(discussions_info)
        self._parse_members_info(members_info)

    def _parse_general_info(self, general_info):
        pass

    def _parse_discussions_info(self, discussions_info):
        pass

    def _parse_members_info(self, members_info):
        self.members_count = members_info['count']
        self.user_ids = members_info['items']
