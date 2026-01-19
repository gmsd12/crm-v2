import os

env = os.getenv("DJANGO_ENV", "local").lower()

if env == "prod":
    from .prod import *  # noqa
else:
    from .local import *  # noqa
