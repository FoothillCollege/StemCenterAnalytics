"""Work in progress script for extracting auto generated emails from the stem center data system."""
import email
import imaplib
from typing import List

import dev_settings
from stem_center_analytics.utils import os_lib, io_lib
from stem_center_analytics import SCRIPT_DIR


def main():
    # Starred messages mean that they were marked as read/attempted to extract/save,
    # but it failed, so their starred to indicate error occurred, and to manually deal with later
    with io_lib.connect_to_imap_server(*dev_settings.IMAP_SERVER_INFO._asdict().values()) as con:
        email_ids = io_lib.get_unread_email_uids(con)
        for email_id in email_ids:
            try:
                io_lib.download_email_attachment(con, email_id, output_dir=SCRIPT_DIR)
                raise ValueError  # if uncommented, will flag error
            except Exception as e:
                print(e)
                print('attachment download failed')
                con.uid('STORE', email_id, '+FLAGS', '\\Flagged')


if __name__ == '__main__':
    main()


# fixme: only mark the email message as read if it was successfully extracted
# todo: figure out how to deal with multiple file attachments, multiple unread, etc...
# todo: figure out how to deal with persisting the temporary attachment through the various scripts

# todo: incorporate the EXPECT_MESSAGE_INFO from dev_settings, and only read one at a time
# mail_client.fetch(email_id, message_set)[0][1] ((BODY.PEEK[]) vs (RFC822))

# the reason we mark are all as read, as otherwise, since the scheduler will run every hour,
# and error ridden ones would be attempted to download again, etc...
