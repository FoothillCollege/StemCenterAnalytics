"""Work in progress script for extracting auto generated emails from the stem center data system."""
import os

import dev_settings  # import the set env variables
from stem_center_analytics import SCRIPT_DIR
from stem_center_analytics.utils import os_lib, io_lib


ATTACHMENTS = ('unclean_tutor_requests', 'unclean_student_logins')  # for use later on


def main():
    # Starred messages mean that they were marked as read/attempted to extract/save,
    # but it failed, so their starred to indicate error occurred, and to manually recover later

    with io_lib.connect_to_imap_server(
        server_host='imap.gmail.com',
        user_name=os.environ.get('EMAIL_SERVER_USERNAME'),
        user_password=os.environ.get('EMAIL_SERVER_PASSWORD')
    ) as con:
        file_names = []
        # get newest unread email, if none then end the script
        email_uids = io_lib.get_unread_email_uids(con, sender=os.environ.get('EMAIL_SENDER'),
                                                  subject='StemCenterData')
        if not email_uids:
            exit(0)
        try:
            file_names = io_lib.download_all_email_attachments(con, email_uids[0], SCRIPT_DIR)
        except Exception as e:
            print(e)  # print traceback
            print('attachment download failed')
            for file_name in file_names:
                os_lib.remove_file(file_name, ignore_errors=True)
            con.uid('STORE', email_uids[0], '+FLAGS', '\\Flagged')  # star the email


if __name__ == '__main__':
    main()


# todo: figure out how to deal with multiple file attachments, multiple unread, etc...
# todo: figure out how to deal with persisting the temporary attachment through the various scripts
# todo: figure out details of logging traceback in the case of failed download
