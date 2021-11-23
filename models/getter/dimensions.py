from libs.caresoft import get_simple

class SimpleGetter:
    def __init__(self, endpoint, row_key):
        self.endpoint = endpoint
        self.row_key = row_key

    def get(self):
        return get_simple(self.endpoint, self.row_key)()

    
