from .wishes_base import WishesBase


class CharacterWishes(WishesBase):
    def init_params(self):
        self.params['gacha_type'] = '301'
        self.file_name = 'genshine_character_wishes.csv'
        self.rst_file_name = 'character_analysis.txt'
        self.table = 'character_wishes'
