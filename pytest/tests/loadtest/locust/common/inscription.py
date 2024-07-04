mport logging
from concurrent import futures
import random
import string
import sys
import pathlib
import typing
from locust import events

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE


class MintInscription(FunctionCall):

    def __init__(self,
                 sender: Account,
                 tick,
                 amt):
        # Attach exactly 1 yoctoNEAR according to NEP-141 to avoid calls from restricted access keys
        super().__init__(sender, "inscription.near", "inscribe", balance=0)
        self.sender = sender
        self.tick = tick
        self.amt = amt

    def args(self) -> dict:
        return {
            "p": "nrc-20",
            "op": "mint",
            "tick": self.tick,
            "amt": str(int(self.amt))
        }

    def sender_account(self) -> Account:
        return self.sender

    @events.init.add_listener
    def on_locust_init(environment, **kwargs):
       INIT_DONE.wait()
       node = NearNodeProxy(environment)
       funding_account = NearUser.funding_account
       parent_id = funding_account.key.account_id
       run_id = environment.parsed_options.run_id
       funding_account.refresh_nonce(node.node)
