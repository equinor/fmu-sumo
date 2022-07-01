#!/usr/bin/env python

import zmq
import os
import socket
import yaml
import hashlib
import base64
from argparse import ArgumentParser
from sumo.wrapper import SumoClient
from res.enkf import ErtScript # type: ignore
from ert_shared.plugins.plugin_manager import hook_implementation # type: ignore



class Uploader:
    def __init__(self, port, env, casepath):
        self.port = port
        self.sumo = SumoClient(env)
        self.casepath = casepath
        self.address = self._get_address()
        self.object_count = 0
        self.failed = 0
        self.success = 0

        self.status = {
            "metadata": {
                "success": 0,
                "failed": 0
            },
            "blob": {
                "success": 0,
                "failed": 0
            } 
        } 

        print(f"Env: {env}")
        print(f"Address: {self.address}")
        print(f"Case path: {self.casepath}")


        self._clean_up()
        self.parent_id = self._upload_case()
        self._listen()


    def _clean_up(self):
        pass

    
    def _listen(self):     
        context = zmq.Context()
        s = context.socket(zmq.PULL)   

        try:
            print(f"Listening on: {self.address}")
            done = False

            s.bind(self.address)

            while not done:
                msg = s.recv_string()

                if msg == "done":
                    done = True
                else:
                    print("Object path received")
                    self.object_count += 1
                    self._upload_child_object(msg)
        except Exception as e:
            print(e)
            s.close()
            context.term()

        print(f"Objects uploaded: {self.object_count}")
        print(self.status)


    def _upload_case(self):
        print("Uploading case metadata")
        
        metadata_path = self.casepath + "/share/metadata/fmu_case.yml"
        metadata = self._parse_yaml(metadata_path)
        result = self.sumo.post("/objects", json=metadata).json()
        case_id = result["objectid"] 

        print(f"Case ID: {case_id}")

        return case_id


    def _upload_child_object(self, path):
        byte_string = self._file_to_byte_string(path)
        yaml_path = self._yaml_path(path)
        metadata = self._parse_yaml(yaml_path)
        digester = hashlib.md5(byte_string)

        metadata["_sumo"] = {
            "blob_size": len(byte_string),
            "blob_md5": base64.b64encode(digester.digest()).decode("utf-8")
        } 

        upload_failed = False

        print(f"Uploading: {path}")

        try:
            print("Uploading metadata..")

            result = self.sumo.post(f"/objects('{self.parent_id}')", json=metadata).json()
            blob_url = result["blob_url"]
            self.status["metadata"]["success"] += 1

            print("Done!")
        except Exception as e:
            upload_failed = True
            self.status["metadata"]["failed"] += 1

            print("Unable to upload metadata")
            print(e)

        if not upload_failed:
            try:
                print("Uploading blob..")

                self.sumo.blob_client.upload_blob(byte_string, blob_url)
                self.status["blob"]["success"] += 1

                print("Done!")
            except Exception as e:
                self.status["blob"]["failed"] += 1

                print("unable to upload blob")
                print(e)


    def _get_address(self):
        hostname = socket.getfqdn()
        local_ip = socket.gethostbyname(hostname)

        return f"tcp://{local_ip}:{self.port}"


    def _parse_yaml(self, path):
        with open(path, "r") as stream:
            data = yaml.safe_load(stream)

        return data


    def _file_to_byte_string(self, path):
        with open(path, "rb") as f:
            byte_string = f.read()

        return byte_string


    def _yaml_path(self, path):
        dir_name = os.path.dirname(path)
        basename = os.path.basename(path)
        relative_path = os.path.join(dir_name, f".{basename}.yml")
        
        return os.path.abspath(relative_path)




def main():
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()

    Uploader(args.port, args.env, args.casepath)


def get_arg_parser():
    arg_parser = ArgumentParser()

    arg_parser.add_argument(
        "port", 
        type=str,
        help="Port to listen on"
    )

    arg_parser.add_argument(
        "env", 
        type=str,
        help="Sumo environment"
    )

    arg_parser.add_argument(
        "casepath", 
        type=str,
        help="Case path"
    )

    return arg_parser


if __name__ == "__main__":
    main()


class SumoReceiver(ErtScript):
    def run():
        main()


@hook_implementation
def legacy_ertscript_workflow(config):
    config.add_workflow(SumoReceiver, "SUMO_RECEIVER")