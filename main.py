import hashlib


class NotWorking(Exception):
    pass


class StockageBase:
    def __init__(self):
        self.working = False
        self._content = {}

    def create(self, key, val):
        self._content[key] = val

    def read(self, key):
        if not self.working:
            raise NotWorking()

        if key not in self._content:
            raise KeyError("Cette valeur n'existe pas")

        return self._content[key]

    def update(self, key, val):
        if key not in self._content:
            raise KeyError("Cette valeur n'existe pas")

        self._content[key] = val

    def delete(self, key):
        if key not in self._content:
            raise KeyError("Cette valeur n'existe pas")

        self._content.pop(key)


class StockageSharding:
    def __init__(self, n):
        self._shards = [StockageBase() for i in range(n)]
        self._n = n

    def create(self, key, val):
        self._shards[hash(key) % self._n].create(key, val)

    def read(self, key):
        return self._shards[hash(key) % self._n].read(key)

    def update(self, key, val):
        self._shards[hash(key) % self._n].update(key, val)

    def delete(self, key):
        self._shards[hash(key) % self._n].delete(key)


hashfunc = lambda x: int(hashlib.sha512(x.encode()).hexdigest()[0:16], 16)


class StockageCH:
    def __init__(self, n):
        self._stores = [StockageBase() for i in range(n)]
        self._ranges = [(2 ** 64 * i // n, 2 ** 64 * (i + 1) // n) for i in range(n)]
        self._n = n

    def _get_ranges(self, key_hash):
        return [
            idx
            for (idx, (inf, sup)) in enumerate(self._ranges)
            if key_hash >= inf and key_hash < sup
        ]

    def create(self, key, val):
        for r in self._get_ranges(hashfunc(key)):
            self._stores[r].create(key, val)

    def read(self, key):
        for r in self._get_ranges(hashfunc(key)):
            try:
                return self._stores[r].read(key)
            except NotWorking:
                continue

        raise KeyError('Aucun noeud contenant la donnée n\'a survécu')

    def update(self, key, val):
        for r in self._get_ranges(hashfunc(key)):
            self._stores[r].update(key, val)

    def delete(self, key):
        for r in self._get_ranges(hashfunc(key)):
            self._stores[r].delete(key)


stockage = StockageSharding(5)
# stockage.read('toto')
stockage.create("toto", 1)
print(stockage.read("toto"))
stockage.update("toto", 2)
print(stockage.read("toto"))
# stockage.update('tutu', 1)
stockage.delete("toto")
# stockage.read('toto')
# stockage.delete('toto')

stockage_ch = StockageCH(50)
stockage_ch.create("toto", 1)
print(stockage_ch.read("toto"))
