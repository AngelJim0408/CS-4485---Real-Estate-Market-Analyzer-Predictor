from fastapi import APIRouter
#from api.database import db_manager
from api.services.data_manager import data_manager
from api.services.updater import update_all

router = APIRouter()

@router.post("/update")
def run_updates():
    data_class = data_manager.get_data_class()

    return update_all(data_class)