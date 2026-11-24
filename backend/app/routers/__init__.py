"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import backup, resource

# 专用路由必须排在通用资源路由之前，避免 /api/{module} 抢占固定路径。
ROUTERS = [backup, resource]
