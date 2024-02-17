# -*- coding: utf-8 -*-
"""
Created on Sat Apr  7 17:35:42 2018

@author: Aaron
"""

import datetime
import json
import logging
import os
import re
from pathlib import Path

import paramiko
import pysftp as sftp

logging.basicConfig(level=logging.INFO)

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


class ServerProcessor:
    def __init__(self, server_name, ip_address, platform):
        self.server_name = server_name
        self.ip_address = ip_address
        self.platform = platform
        self.username = "appuser"  # has some limited specific sudo usage
        self.directory = "/home/myappuser/scripts/"
        self.server_num = self.get_server_num()
        self.private_key = self.get_key()

    def put_files_to_server(self, filenames):
        try:
            s = sftp.Connection(
                host=self.ip_address,
                username=self.username,
                private_key=self.private_key,
                cnopts=cnopts,
            )
            for filename in filenames:
                localPath = filename
                remotePath = self.directory + filename
                s.put(localPath, remotePath)
            logging.info(f"Moved file(s) to {self.server_name}")
            s.close()
        except Exception as e:
            logging.error(f"Failed to upload file(s) to {self.server_name}: {e}")

    def get_files_from_server(self, folder_names):
        try:
            s = sftp.Connection(
                host=self.ip_address,
                username=self.username,
                private_key=self.private_key,
                cnopts=cnopts,
            )
            for folder in folder_names:
                files = s.listdir(folder)
                for file in files:
                    localPath = "raw_data/" + filename + "/" + file
                    remotePath = self.directory + filename + "/" + file
                    s.get(remotePath, localPath)
            logging.info(f"Retrieved file(s) from {self.server_name}")
            s.close()
        except Exception as e:
            logging.error(f"Failed to retrieve file(s) from {self.server_name}: {e}")

    def give_commands(self, commands):
        """
        The parameter "commands" is either a dictionary or a list. It contains
        commands to be passed through to the command line.
        If interactive commands are required, it is a dictionary, with each
        key command having its own interactive commands, e.g. when a [Y/N] is
        prompted.
        When interactive commands are not used, a list of commands is passed instead.
        """
        k = paramiko.RSAKey.from_private_key_file(self.private_key)
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        logging.info(f"Connecting to {self.server_name}...")
        c.connect(hostname=self.ip_address, username=self.username, pkey=k)
        logging.info("Connected.")

        for command in commands:
            logging.info(f"Executing {command}")
            stdin, stdout, stderr = c.exec_command(command)
            if isinstance(commands, dict) and commands[command]:
                for interactiveCommand in commands[command]:
                    logging.info(f"Entering {interactiveCommand}")
                    stdin.write(interactiveCommand + "\n")
                    stdin.flush()
            output = stdout.read().decode()
            errors = stderr.read().decode()
            if output:
                logging.info(f"Output: {output}")
            if errors:
                logging.error(f"Errors: {errors}")
        c.close()

    def get_key(self):
        if self.platform == "DigitalOcean":
            return os.getenv("ACCESS_KEY_DO")
        elif self.platform == "AWS":
            return os.getenv("ACCESS_KEY_AWS")

    def get_server_num(self):
        return re.findall(r"\d+", self.server_name)[0]

    def get_orderbook_collection_commands(self):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        stdout_log = f"orderbook_{self.server_num}_stdout_{date_str}.log"
        stderr_log = f"orderbook_{self.server_num}_stderr_{date_str}.log"

        commands = [
            f"nohup python3 orderbook_hdf.py {self.server_num} > {stdout_log} 2> {stderr_log} < /dev/null &"
        ]
        return commands

    def push_scripts_to_server(self):
        files = [
            "orderbook_hdf.py",
            "initialise_exchanges.py",
            "get_trading_symbols.py",
            "setup.sh",
        ]
        commands = {"bash setup.sh": "y"}
        self.put_files_to_server(files)
        self.give_commands(commands)
        logging.info(f"self.{server_name} set up successfully.")

    def run_orderbook_collection(self):
        commands = self.get_orderbook_collection_commands()
        self.give_commands(commands)

    def kill_python3(self):
        self.give_commands(["sudo pkill python3"])

    def clear_orderbook_folder(self):
        commands = ["rm -f orderbook" + str(self.server_num) + "/*"]
        self.give_commands(commands)

    def kill_python3_and_clear_orderbook(self):
        self.kill_python3()
        self.clear_orderbook_folder()

    def remove_orderbook_folder(self):
        commands = ["rm -r orderbook" + str(self.server_num) + "/*"]
        self.give_commands(commands)

    def get_orderbook_files(self):
        self.get_files_from_server(folder_names=[f"orderbook{self.server_num}"])


class AllServers:
    def __init__(self, dict_of_servers):
        self.servers = self.get_server_objects()

    def __getattr__(self, name):
        def method(*args, **kwargs):
            results = []
            for server in self.servers:
                # Check if the ServerProcessor object has the attribute and is callable
                if hasattr(server, name) and callable(getattr(server, name)):
                    result = getattr(server, name)(*args, **kwargs)
                    results.append(result)
            return results

        return method

    def get_server_objects(self):
        server_objects = []
        for svr_name, details in dict_of_servers.items():
            ip_address = details["ip"]
            platform = details["platform"]
            server_objects.append(
                ServerProcessor(
                    server_name=svr_name, ip_address=ip_address, platform=platform
                )
            )
        return server_objects


if __name__ == "__main__":
    with open("server_config.json", "r") as file:
        servers_dict = json.load(file)

    server0 = ServerProcessor(
        server_name="server0",
        ip_address=servers_dict["server0"]["ip"],
        platform=servers_dict["server0"]["platform"],
    )

    all_servers = AllServers(servers_dict)

    """
    to test servers can be reached:
    """
    # all_servers.give_commands(commands=['\n'])

    """
    to erase data currently on one server to start fresh:
    """
    # server0.kill_python3_and_clear_orderbook()

    """
    to erase data currently on servers to start fresh:
    """
    # all_servers.kill_python3_and_clear_orderbook()

    """
    to kill all python processes across servers:
    """
    # all_servers.kill_python3()

    """
    to upload relevant scripts on to one server:
    """
    # server0.push_scripts_to_server()

    """
    to upload relevant scripts on to all servers:
    """
    # all_servers.push_scripts_to_server()

    """
    to run orderbook collection on a single test server:
    """

    # server0.run_orderbook_collection()

    """
    to run orderbook collection script on all servers:
    """
    # all_servers.run_orderbook_collection()

    """
    to collect orderbook folders from all servers:
    """
    # all_servers.get_orderbook_files()

    pass
