"""CLI 层：rich 看板与跨平台键盘。

注意：入口函数 ``main`` 位于 ``quant.cli.main``（其依赖
:class:`quant.app.QuantApp`），不在本包 ``__init__`` 导出，以避免
「app → cli.dashboard → cli.__init__ → main → app」循环导入。
"""

from quant.cli.dashboard import DashboardRenderer, DashboardState, SymbolView
from quant.cli.keyboard import KeyboardController

__all__ = [
    "DashboardRenderer",
    "DashboardState",
    "KeyboardController",
    "SymbolView",
]
