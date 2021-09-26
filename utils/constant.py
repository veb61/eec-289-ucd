class MetaConst(type):
    def __getattr__(cls, key):
        return cls[key]

    def __setattr__(cls, key, value):
        if key[0] == '_':
            super().__setattr__(key, value)
        else:
            raise TypeError


class Const(object, metaclass=MetaConst):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        if name[0] == '_':
            super().__setattr__(name, value)
        else:
            raise ValueError("setattr while locked", self)