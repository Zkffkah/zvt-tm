# -*- coding: utf-8 -*-
import functools

from zvt import init_config



zvt_coin_config = {}

int_zvt_coin_config = functools.partial(init_config, pkg_name='zvt_coin', current_config=zvt_coin_config)

int_zvt_coin_config()

# the __all__ is generated
__all__ = []

# __init__.py structure:
# common code of the package
# export interface in __all__ which contains __all__ of its sub modules

# import all from submodule fill_project
from .fill_project import *
from .fill_project import __all__ as _fill_project_all
__all__ += _fill_project_all

# import all from submodule recorders
from .recorders import *
from .recorders import __all__ as _recorders_all
__all__ += _recorders_all

# import all from submodule domain
from .domain import *
from .domain import __all__ as _domain_all
__all__ += _domain_all