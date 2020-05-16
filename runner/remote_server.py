#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sshtunnel

def open_remote_port():
    db_host = "r-synergy.com"
    ssh_username="toshick"
    ssh_private_key_password=""
    ssh_pkey="~/.ssh/id_rsa"
    
    server = sshtunnel.SSHTunnelForwarder(
                (db_host, 22),
                ssh_username=ssh_username,
                ssh_private_key_password=ssh_private_key_password,
                ssh_pkey=ssh_pkey,
                remote_bind_address=('127.0.0.1', 3306))
    
    server.start()
    return server, server.local_bind_port

def close_remote_port(server):
    server.stop()
