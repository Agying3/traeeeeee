"""Haozhu management commands; shares settings and H5 client with WebUI."""
import argparse
import getpass
import json

import haozhu_sms as hz
from haozhu_catalog import Catalog, CatalogError, save_config, select_channels, uid_list


def main(argv=None):
    parser = argparse.ArgumentParser(description="Haozhu projects and channel management")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Show local configuration status without secrets")
    session = sub.add_parser("session", help="Save H5 PHPSESSID using hidden input")
    session.add_argument("--clear", action="store_true")
    sub.add_parser("search", help="Search projects").add_argument("keyword")
    sub.add_parser("uids", help="List market channels, prices and stock").add_argument("sid")
    sub.add_parser("my-uids", help="List joined channels").add_argument("keyword", nargs="?", default="")
    sub.add_parser("add-uid", help="Join a channel in the provider account").add_argument("uid")
    select = sub.add_parser("select", help="Save local project/channel selection")
    select.add_argument("sid", help="Numeric project ID")
    select.add_argument("uids", nargs="*", help="UIDs to use, empty clears selection")
    select.add_argument("--append", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            creds = hz.load_credentials()
            result = {"sid": creds.get("sid", ""), "uids": uid_list(creds.get("uids", "")),
                      "uid": creds.get("uid", ""), "h5_configured": bool(creds.get("h5_session"))}
        elif args.command == "session":
            value = "" if args.clear else getpass.getpass("H5 PHPSESSID: ").strip()
            if not args.clear and not value:
                parser.error("PHPSESSID is empty")
            result = save_config({"h5_session": value})
        elif args.command == "select":
            result = select_channels(args.sid, args.uids, args.append)
        else:
            client = Catalog()
            if args.command == "search":
                result = {"items": client.projects(args.keyword)}
            elif args.command == "uids":
                result = {"items": client.channels(args.sid)}
            elif args.command == "my-uids":
                result = {"items": client.channels(keyword=args.keyword, mine=True)}
            else:
                result = client.add_uid(args.uid)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (CatalogError, OSError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        return 1
