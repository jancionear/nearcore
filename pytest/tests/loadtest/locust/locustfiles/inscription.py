import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger
from locust import constant_throughput, task
from common.base import NearUser
from common.inscription import MintInscription
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE, Transaction
from locust import events
logger = new_logger(level=logging.WARN)


class MintInscriptionUser(NearUser):

    @task
    def mint(self):
        self.send_tx(MintInscription(self.account, "abahmane-meme", amt=100), locust_name="Mint Inscription")

    def on_start(self):
        #makes a user
        super().on_start()
        #self.account = self.environment.account

    @events.init.add_listener
    def on_locust_init(environment, **kwargs):
        INIT_DONE.wait()
        node = NearNodeProxy(environment)
        funding_account = NearUser.funding_account
        parent_id = funding_account.key.account_id
        funding_account.refresh_nonce(node.node)
        #environment.account = Account(parent_id)
