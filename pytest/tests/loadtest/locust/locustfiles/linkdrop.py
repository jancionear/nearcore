"""
A workload with Linkdrop operations.
"""

import logging
import pathlib
import random
import sys
import ed25519
import base58
import string

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key

from configured_logger import new_logger
from locust import constant_throughput, task
#from common.base import NearUser
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE
from common.linkdrop import LinkdropContract, AddKey, ClaimDrop

logger = new_logger(level=logging.WARN)


class LinkdropUser(NearUser):
    """
    Funder creates a drop on Keypom contract. Add keys to the drop.
    Claimer claims the drop.
    """
    # Only simple claims right now. Do FT, NFT etc later.

    # Each Locust user will try to send one transaction per second.
    # See https://docs.locust.io/en/stable/api.html#locust.wait_time.constant_throughput.
    wait_time = constant_throughput(1.0)

    @task
    def create_and_claim_drop(self):
        # Generate a random key pair
        private_key, public_key = ed25519.create_keypair()
        # Convert keys to Base58 and add prefix
        sk = 'ed25519:' + base58.b58encode(private_key.to_bytes()).decode('ascii')
        pk = 'ed25519:' + base58.b58encode(public_key.to_bytes()).decode('ascii')
        public_keys = []
        public_keys.append(pk)
        # print("Limited Secret Key - ", sk)
        # print("Limited Publick Key - ", pk)
        #need drop id also in case of keypom, or use original linkdrop contract
        #print(self.linkdrop.account, self.account,self.linkdrop.account, public_keys, self.drop_id)
        #print("self.account in AddKey which is the sender/signer of it.", self.account.key.account_id)
        tx = AddKey(self.linkdrop.account, self.account, public_keys, self.drop_id)
        self.send_tx_async(tx, locust_name="Key added to the drop.")
        #print(result)
        # near_name = f"{random_string()}.near"
        # # print(near_name)
        # private_key, public_key = ed25519.create_keypair()
        # # # Convert keys to Base58 and add prefix
        # sk = 'ed25519:' + base58.b58encode(private_key.to_bytes()).decode('ascii')
        # pk = 'ed25519:' + base58.b58encode(public_key.to_bytes()).decode('ascii')
        # # print("New Secret Key - ", sk)
        # # print("New Publick Key - ", pk)
        # #self.pk = pk
        # node = NearNodeProxy(self.environment)
        # print("Going into Claim Now")
        # tx_2 = ClaimDrop(self.linkdrop.account, near_name, pk,sk, node.node)
        # result2 = self.send_tx(tx_2, locust_name="Linkdrop Claimed")
        #print(result2)


    #Create a simple drop on Linkdrop contract
    def on_start(self):
        #makes a user
        super().on_start()
        self.linkdrop = random.choice(self.environment.linkdrop_contracts)
        #print("Inside on_start, self.linkdrop - ", self.linkdrop.account.key.account_id)
        #self.ft = random.choice(self.environment.ft_contracts)
        # Keypom contract does not need registration and distibution funds like FT.
        # Just create a drop.
        # Do I need to create a user first or something?
        self.drop_id = self.linkdrop.create_drop(self)
        #self.ft.register_user(self)
        logger.debug(
            f"{self.account_id} ready to use Linkdrop contract {self.linkdrop.account.key.account_id}"
        )

def random_string(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Event listener for initializing Locust.
from locust import events

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    INIT_DONE.wait()
    node = NearNodeProxy(environment)
    linkdrop_contract_code = environment.parsed_options.linkdrop_wasm
    num_linkdrop_contracts = environment.parsed_options.num_linkdrop_contracts
    funding_account = NearUser.funding_account
    parent_id = funding_account.key.account_id
    #print("Funding Account - ",parent_id)

    #print("I am in INIT")
    funding_account.refresh_nonce(node.node)

    environment.linkdrop_contracts = []
    # TODO: Create accounts in parallel
    for i in range(num_linkdrop_contracts):
        account_id = environment.account_generator.random_account_id(
            parent_id, '_linkdrop')
        contract_key = key.Key.from_random(account_id)
        linkdrop_account = Account(contract_key)
        #print("Linkdrop account before being deployed", linkdrop_account.key.account_id)
        linkdrop_contract = LinkdropContract(linkdrop_account, linkdrop_account, linkdrop_contract_code)
        linkdrop_contract.install(node, funding_account)
        #print("Linkdrop Contract - ",linkdrop_contract.account.key.account_id)
        environment.linkdrop_contracts.append(linkdrop_contract)


# FT specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--linkdrop-wasm",
                        default="res/keypom.wasm",
                        help="Path to the compiled LinkDrop (Keypom) contract")
    parser.add_argument(
        "--num-linkdrop-contracts",
        type=int,
        required=False,
        default=4,
        help=
        "How many different Linkdrop contracts to spawn from this worker (Linkdrop contracts are never shared between workers)"
    )
