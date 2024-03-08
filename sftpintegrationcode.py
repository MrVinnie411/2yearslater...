# Standard Library
import csv
import json
import logging
import os
import posixpath
import subprocess
import tempfile
from datetime import datetime

# Third Party
import pandas as pd
import pysftp
from six import BytesIO, StringIO
from typing import Any, AnyStr, Dict, List, Optional, Tuple, Union

# Framework
from django.utils import timezone



log = logging.getLogger(__name__)


@patch_dsa
def upload_fo_to_ftp(
    file_object,                # type: BytesIO
    file_name,                  # type: AnyStr
    ftp_endpoint,               # type: AnyStr
    ftp_username,               # type: AnyStr
    ftp_password,               # type: AnyStr
    ftp_dir=None,               # type: Optional[AnyStr]
    overwrite_file=False,       # type: Optional[bool]
):
    """
    Login to the specified ftp server and upload the file whose path(s) is/are provided.

    Arguments:
        file_object (BytesIO | StringIO): a file-like object to upload
        file_name (str): the remote name the file should go to
        ftp_endpoint (str): The Endpoint URL of the FTP server
        ftp_dir: The directory on the FTP server, relative to the home directory of the user
        ftp_username: The username of the user on the FTP Server
        ftp_password: The password on the FTP server
        overwrite_files (opt bool): Whether to overwrite any existing files

    Raises:
        ValueError: If a file already exists in the destination and overwrite_files is false OR
            if the local file doesn't actually exist

    Returns:
        success(bool): The result of the FTP upload.
    """

    if not isinstance(file_object, (BytesIO, StringIO)):
        raise ValueError("The provided file object isn't a file object, it is a {}!".format(type(file_object)))

    # don't check for hostkey as we log in to sftp
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # TODO: Switch to using known host keys
    # Use a login credential dictionary to facilitate required future extension to SSH keys.
    login_creds = {
        "username": ftp_username
    }
    login_creds["password"] = ftp_password

    # open a SFTP connection and cd into the ftp dir
    with pysftp.Connection(
            ftp_endpoint,
            cnopts=cnopts,
            **login_creds) as sftp:
        if ftp_dir:
            sftp.chdir(ftp_dir)
        if not overwrite_file and sftp.isfile(file_name):
            raise ValueError("File {} already exists on the destination!".format(file_name))
        file_object.seek(0)
        sftp.putfo(flo=file_object, remotepath=file_name)
    return True


def convert_records_to_file_obj(records, filetype):
    # type (Dict, str) -> StringIO, str
    """
    Writes the record as the given file type if supported, else raises error

    Args:
        records (dict): the records as a queryset of dictionaries
        filetype (str): The type of file to convert to
    Returns:
        file_obj(StringIO)
        content_type (bytes str): the content type to be passed to the uploader
    """
    file_obj = StringIO()
    if filetype == 'csv':
        pd.DataFrame.from_records(records).to_csv(file_obj, encoding='UTF-8', index=False)
        content_type = b'text/csv'
    elif filetype == 'json':
        if not isinstance(records, list):
            records = list(records)  # QuerySets are not by json, who knew
        json.dump(records, file_obj)
        content_type = b'application/json'
    else:
        raise ValueError("Invalid file type: {}".format(filetype))

    file_obj.seek(0)

    return file_obj, content_type


def upload_records_to_external_ftp(
    records,                    # type: str
    filetype,                   # type: str
    file_name,                  # type: str
    ftp_endpoint,               # type: str
    ftp_username,               # type: str
    ftp_password,               # type: str
    ftp_dir=None,               # type: Optional[str]
    overwrite_file=False,       # type: bool
):
    # type (...) -> bool
    """
    Wrapper function to allow easy uploading of a Dictionary to an external FTP server
    If you are looking to upload to Quorum's FTP server, use app.custom_data.functions.QuorumSFTPHelper

    Args:
        records (dict): the records as a queryset of dictionaries
        filetype (str): The type of file to convert to
        file_name (str): the remote name the file should go to
        ftp_endpoint (str): The Endpoint URL of the FTP server
        ftp_dir: The directory on the FTP server, relative to the home directory of the user
        ftp_username: The username of the user on the FTP Server
        ftp_password: The password on the FTP server
        overwrite_files (opt bool): Whether to overwrite any existing files

    returns:
        bool: Whether the upload succeeded or not
    """
    file_obj, _ = convert_records_to_file_obj(records, filetype)
    success = upload_fo_to_ftp(
        file_object=file_obj,
        file_name=file_name,
        ftp_endpoint=ftp_endpoint,
        ftp_username=ftp_username,
        ftp_password=ftp_password,
        ftp_dir=ftp_dir,
        overwrite_file=overwrite_file
    )
    return success


class NewSFTPIntegration(NewIntegration):
    """
    New home for SFTP-Based Integrations
    """
    INTEGRATION_TYPE = IntegrationType.sftp

    def __init__(
        self,
        config_id,                          # type: int
        dry_run=True,                       # type: bool
        force=False,                        # type: bool
        dummy_multi=False,                  # type: bool
        days_back=None,                     # type: Optional[int]
        minutes_back=None,                  # type: Optional[int]
        back_to_exact_date=None             # type: Optional[datetime]
    ):
        # type: (...) -> None
        """
        Instantiate a New Integration instance and update the Integration Data Types dictionary for the
        types supported by this integration (will be used for settings validation)

        Args:
            config_id: The ID of the IntegrationConfiguration object to instantiate for
            dry_run (Opt Boolean): Whether to run this as a dry run or not; defaults True to avoid unintended actual runs
            force (bool): For any records that aren't updated due to a comparison between Quorum and External System (e.g. external
                is newer, etc.), override and force the update; for SFTP specifically, this includes forcing the processing
                of files that haven't changed since they were last processed; intended to be used as an argument when running manually in the shell
            dummy_multi (opt Boolean): Whether to actually run multiprocessed or not, defaults False; intended to be used as an argument
                when running manually in the shell
            days_back (opt Int): In searching for data that has been updated since datetime X, set datetime X to this many days before now;
                intended to be used as an argument when running manually in the shell
            minutes_back (opt Int): In searching for data that has been updated since datetime X, set datetime X to this many minutes before now;
                intended to be used as an argument when running manually in the shell
            back_to_exact_date (opt datetime): Run the integration back to the specific date/time specified in string format;
                intended to be used as an argument when running manually in the shell
        """
        super(NewSFTPIntegration, self).__init__(
            config_id=config_id,
            force=force,
            dry_run=dry_run,
            dummy_multi=dummy_multi,
            days_back=days_back,
            minutes_back=minutes_back,
            back_to_exact_date=back_to_exact_date
        )

        self.SUPPORTED_QUORUM_OBJECTS.update({
            Amendment: SyncAllowedDataDirection.only_export_from_quorum,
            Bill: SyncAllowedDataDirection.bidirectional,
            BulkEmail: SyncAllowedDataDirection.only_export_from_quorum,
            BulkSMS: SyncAllowedDataDirection.only_export_from_quorum,
            Campaign: SyncAllowedDataDirection.only_export_from_quorum,
            Committee: SyncAllowedDataDirection.only_export_from_quorum,
            ConfirmationEmail: SyncAllowedDataDirection.only_export_from_quorum,
            CustomData: SyncAllowedDataDirection.only_import_to_quorum,
            GrassrootsRegistrationPage: SyncAllowedDataDirection.only_export_from_quorum,
            GrassrootsSupporterAction: SyncAllowedDataDirection.only_export_from_quorum,
            IssueManagement: SyncAllowedDataDirection.only_export_from_quorum,
            MessageEvent: SyncAllowedDataDirection.only_export_from_quorum,
            NewStaffer: SyncAllowedDataDirection.bidirectional,
            Note: SyncAllowedDataDirection.bidirectional,
            Official: SyncAllowedDataDirection.bidirectional,
            PressContact: SyncAllowedDataDirection.bidirectional,
            PublicOrganization: SyncAllowedDataDirection.bidirectional,
            Regulation: SyncAllowedDataDirection.only_export_from_quorum,
            SendEmail: SyncAllowedDataDirection.only_export_from_quorum,
            SendSMS: SyncAllowedDataDirection.only_export_from_quorum,
            Supporter: SyncAllowedDataDirection.bidirectional,
            Vote: SyncAllowedDataDirection.only_export_from_quorum
        })

        # Account for the transition from old, with all settings on the config_dict, to new, with separate fields
        if self.config.flat_file_source_type:
            self.server_type = self.config.flat_file_source_type
            self.quorum_sftp_base_path = self.config.quorum_sftp_base_path.replace('{{datetime}}', timezone.now().strftime('%Y_%m_%d'))
            self.quorum_sftp_user_id = self.config.quorum_sftp_user_id
        else:
            sftp_settings = self.config_dict.get("SFTP Settings")
            self.server_type = FlatFileSourceType.by_label(sftp_settings.get("Server Type"))
            self.quorum_sftp_base_path = sftp_settings.get("Folder Path").replace('{{datetime}}', timezone.now().strftime('%Y_%m_%d'))
            self.quorum_sftp_user_id = sftp_settings.get("SFTP User ID")
        self.aws_ssm_path_to_credentials = self.config.aws_ssm_path_to_credentials or self.config_dict.get("SSM Path for External Credentials")

    def convert_quorum_df_to_external_df(self, task_name, quorum_df):
        # type: (str, pd.DataFrame) -> pd.DataFrame
        """
        Convert the Quorum Dataframe to the External Dataframe by (in order):
            1) Cleaning the dataframe using the standard function
            - Nothing else required for SFTP syncs

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync
            quorum_df (DataFrame): The dataframe to convert from quorum to external CRM

        Returns:
            dataframe: The converted dataframe
        """
        task = self.all_tasks.get(task_name)

        # 1) Clean Dataframe
        converted_df = self.clean_df_columns(quorum_df, fields_mapping=task.fields_mapping, map_to_quorum=False)
        return converted_df

    def send_external_df_to_external(self, task_name, external_df):
        # type: (str, pd.DataFrame) -> Any
        """
        Stub method that sends a Dataframe of Quorum-originated data to an external system

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync
            external_df (DataFrame): The dataframe to send to the external CRM

        Returns:
            Bool: Did the upload run successfully
        """
        task = self.all_tasks[task_name]

        if "{{datetime}}" in task.sftp_file_name:
            current_datetime = timezone.now().strftime('%Y_%m_%d')
            filename = task.sftp_file_name.replace("{{datetime}}", current_datetime)
        elif "{{timestamp}}" in task.sftp_file_name:
            current_datetime = timezone.now().strftime('%Y_%m_%d_%H_%M')
            filename = task.sftp_file_name.replace("{{timestamp}}", current_datetime)
        else:
            filename = task.sftp_file_name

        if self.dry_run:
            self.save_dataframe_locally(output_df=external_df, filename=filename)
            log.info("{} successfully uploaded locally".format(filename))
            return ["Success"]

        if "quorum_side_primary_key" in external_df.columns:
            # This shouldn't be uploaded for real, so drop it at this point
            external_df = external_df.drop(columns="quorum_side_primary_key")

        results = self.upload_to_remote(external_df, filename)
        return results

    def get_external_df_from_external(self, task_name):
        # type: (str) -> pd.DataFrame
        """
        Convert data downloaded as a local file from an SFTP Server into a dataframe

        Args:
            task_name (str): The task name (as specified in the Configuration) to get

        Returns:
            dataframe: The external data, loaded into a Dataframe
        """

        task = self.all_tasks.get(task_name)

        # Sometimes files need to be rewritten (encryption, crappy data), so we do that here
        filepath = rewrite_file(self.organization.id, self.current_local_filepath)

        log.debug("Discovering file encoding/delimiter")
        file_extension = get_file_extension(filepath)

        # If we KNOW the delimiter, we should just use it, otherwise try to find it (this is very fast, even with large files)
        delimiter = task.sftp_delimiter or find_csv_delimiter(filepath)

        # Sometimes files come in with character encoding that isn't UTF-8 compatible, but discovering that encoding can be
        # VERY VERY slow on large files; so just try UTF-8 loading first and if it fails, then use the detector
        try:
            log.debug(u"Attempting to load {} as a Pandas dataframe".format(filepath))
            dataframe = load_spreadsheet_as_dataframe(
                filepath,
                file_extension.key,
                delimiter=delimiter,
                encoding="UTF-8",
                dtype=object,
                fail_on_unicode_error=True
            )
        except (UnicodeDecodeError, UnicodeError) as e:
            # TODO: Log this somewhere so that we can reach out to the client if needed to discuss their file encoding format
            log.warning(u"File for config {} failed to load with UTF-8 encoding, error {}".format(
                self.config,
                e
            ))
            encoding = find_csv_encoding(filepath)
            log.debug(u"Attempting to load {} as a Pandas dataframe".format(filepath))
            dataframe = load_spreadsheet_as_dataframe(
                filepath,
                file_extension.key,
                delimiter=delimiter,
                encoding=encoding,
                dtype=object,
                fail_on_unicode_error=True
            )
        except Exception as e:
            log.error(u"P4 File for config {} failed to load, error {}".format(
                self.config,
                e,
            ), extra={
                "to_sentry": True,
                "runbook": "https://docs.google.com/document/d/1wm8Zh0zVDy_GEsyZFcz4R9jkvfFnXoZKNSKaj1WqwfA/edit#heading=h.1ivvv4utfxux",
            })

        return dataframe

    def convert_ext_df_to_quorum_df(self, task_name, external_df):
        # type: (str, pd.DataFrame) -> pd.DataFrame
        """
        Convert the External Dataframe to a Quorum Dataframe ready to upload to Quorum

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync
            external_df (DataFrame): The external dataframe to convert to a Quorum dataframe

        Returns:
            dataframe: The Dataframe ready to import into Quorum
        """

        task = self.all_tasks[task_name]
        fields_mapping = task.fields_mapping

        preprocessors_list = task.external_processors

        log.info(u"Running preformatters (if any) for {}, integration configuration {}".format(self.organization, self.config.id))

        # Add the literal fields, if any
        external_df = self.apply_literal_fields(dataframe=external_df, is_quorum_import=True, task_name=task_name)

        # Run the preprocessors
        for preprocessor_name, argument in preprocessors_list:
            preprocessor_class = get_sftp_external_preprocessor(preprocessor_name)
            if preprocessor_class:
                preprocessor = preprocessor_class(self)
                log.info(u"Applying preprocessor {} for task {}".format(preprocessor_name, task_name))
                external_df = preprocessor.apply(external_df, argument)

            if external_df.empty:
                log.info(u"Dataframe has been emptied by {} on {}!".format(preprocessor_name, argument))
                return external_df

        external_df = self.clean_df_columns(dataframe=external_df, fields_mapping=fields_mapping, map_to_quorum=True)

        return external_df

    def run_task_from_external_crm_to_quorum(self, task_name):
        # type: (str) -> Any
        """
        Run task from an External SFTP System to Quorum

        NOTE: Unlike other systems, SFTP imports can result in multiple files being downloaded at the same time.  As a result
        the structure of this function is SLIGHTLY different - there is a "pre-step" which actually retrieves the file(s) from
        the appropriate SFTP resource, and then the standard get_external -> convert_external -> send_to_quorum workflow
        takes over in a loop for each file downloaded.

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync

        Returns:
            bool: A boolean for whether the sync succeeded entirely or not
        """

        # Go find out how many new files exist to be processed
        filepaths_with_change = self._get_sftp_filepaths_with_change(task_name)
        if not filepaths_with_change:
            # TODO: Grafana Log an attempt to get a file with no files found
            return []

        some_change = False
        results = []    # type: List[Union[bool, Dict]]
        for local_filepath, has_changed in filepaths_with_change:
            # Only bother processing a file when it has actually changed; most of the time the latest file was already
            # processed so no need to process it again
            if has_changed or self.force or self.dry_run:
                self.current_local_filepath = local_filepath
                # First, download the dataframe from the external system (which may be the Quorum SFTP server or an external server)
                external_df = self.get_external_df_from_external(task_name=task_name)

                # Convert the external dataframe into a Quorum dataframe by running client-specific preprocessors
                converted_df = self.convert_ext_df_to_quorum_df(task_name=task_name, external_df=external_df)

                if self.dry_run:
                    # It helps to also have the quorum_headers that would be imported
                    quorum_headers = self.quorum_side_helper._make_quorum_headers(task=self.all_tasks[task_name])
                    results.append({"Quorum Headers": quorum_headers, "Dataframe": converted_df})
                else:
                    # Send the Quorum Dataframe into Quorum using the code in the Quorum-Side Helper
                    results.append(self.quorum_side_helper.send_df_to_quorum(task_name=task_name, quorum_df=converted_df))
                    # Since we have run this file successfully, update its Checksum
                    if has_changed:
                        # We only need to save the checksum if the file actually changed, not if we are forcing
                        FileManager.save_file(
                            local_filepath,
                            sftp_configuration=None,
                            integration_configuration=self.config
                        )
                some_change = True

        if not some_change:
            # TODO: Grafana Log files found but none changed.
            log.warning(u"File(s) {} for sync {} has NOT changed".format(
                [file_bool[0] for file_bool in filepaths_with_change],
                self.config
            ))

        # Remove the local copy of the file after we finish loading it in as a BulkUploadFile object
        for filepath, _ in filepaths_with_change:
            os.remove(filepath)

        if self.dry_run:
            # Dry run, just return the results
            return results

        if False in results:
            # At least one of the uploads to Quorum failed, so return False
            return False

        return True

    def run_task_from_quorum_to_external_crm(self, task_name):
        # type: (str) -> bool
        """
        Sync(upsert) the data from Quorum to the SFTP site

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync

        Returns:
            Bool: Was it successful
        """
        log.info("Running {} from Quorum to External".format(task_name))
        quorum_df = self.quorum_side_helper.get_df_from_quorum(
            task_name=task_name,
        )
        if not isinstance(quorum_df, pd.DataFrame) or quorum_df.empty:
            # Didn't find anything to send, continue
            log.warning("No results returned from Quorum for task {}".format(task_name))
            return False
        external_df = self.convert_quorum_df_to_external_df(quorum_df=quorum_df, task_name=task_name)
        self.send_external_df_to_external(task_name=task_name, external_df=external_df)
        return True

    @staticmethod
    def get_org_details(org_id):
        """
        Fetches the public key and recipient email for a given organization ID.

        Parameters:
        - org_id (int): The organization ID.

        Returns:
        - dict: Contains 'public_key' and 'recipient_email' if the organization ID is found.
        - None: If the organization ID is not found.
        """
        from app.userdata.constants import ORG_DETAILS
        return ORG_DETAILS.get(org_id, None)

    def encrypt_data(self, data):
        """
        Encrypts the given data using Client's public key.

        This function is originally designed to work with Google integration.
        It has now been universalized for use by any client going forward
        It performs the following steps:
        1. Imports Client's public GPG key into a temporary keyring.
        2. Encrypts the data using this public key.
        3. Reads and returns the encrypted data.

        Parameters:
        - data (str or bytes): The data to be encrypted.

        Returns:
        - bytes: The encrypted data.
        - None: If the organization does not have a GPG key and recipient email in app.userdata.constants

        Raises:
        - Various exceptions can be raised by the subprocess and file operations.
        """
        import gnupg

        # Fetch organization details based on self.organization.id
        org_details = self.get_org_details(org_id=self.organization.id)

        if not org_details:
            log.info("No encryption details found for this organization.")
            return None

        public_key = org_details['public_key']
        recipient_email = org_details['recipient_email']

        # a workaround for a specific issue with the python-gnupg library and the way it handles certain GPG status messages.
        gnupg._parsers.Verify.TRUST_LEVELS["ENCRYPTION_COMPLIANCE_MODE"] = 23

        # Create a temporary file to store the public key
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as key_file:
            key_file.write(public_key)
            key_file_name = key_file.name

        # Import the public key into the GPG keyring
        import_command = ['gpg', '--import', key_file_name]
        subprocess.call(import_command)

        # Create another temporary file to store the encrypted data
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            output_file_name = temp_file.name

        # Use a subprocess to run the gpg command, passing the CSV data directly
        encrypt_command = [
            'gpg',
            '--encrypt',
            '--trust-model', 'always',
            '--yes',
            '--recipient', recipient_email,
            '--output', output_file_name,
            '-'
        ]
        encrypt_process = subprocess.Popen(encrypt_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        _, encrypt_stderr = encrypt_process.communicate(input=data.encode('utf-8') if isinstance(data, unicode) else data)

        if encrypt_process.returncode != 0:
            log.error("Error encrypting file:", encrypt_stderr.decode())
            return

        # Read the encrypted data from the temporary file
        with open(output_file_name, 'rb') as file:
            encrypted_data = file.read()

        # Remove the temporary files
        os.remove(output_file_name)
        os.remove(key_file_name)

        return encrypted_data

    def save_dataframe_locally(self, output_df, filename):
        """
        Saves the output_df as a given filetype locally

        Args:
            output_df (DataFrame): the output dataframe
            filename (str): Formatted filename that includes the filetype

        Returns:
            bool: Did it succeed
        """

        if filename.endswith('.csv.gpg'):
            # Convert DataFrame to CSV format
            buffer = StringIO()  # For Python 2.7 compatibility
            output_df.to_csv(buffer, encoding='utf-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
            csv_data = buffer.getvalue()

            # Encrypt the CSV data
            encrypted_data = self.encrypt_data(csv_data)
            if encrypted_data is None:
                log.error("Encryption failed.")
                return False

            # Save encrypted data to file
            with open(filename, 'wb') as file:
                file.write(encrypted_data)
            log.debug("Encrypted data written to file: {}".format(filename))
            return True

        elif filename.endswith('.csv'):
            output_df.to_csv(filename, encoding='UTF-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
            log.debug("CSV data written to file: {}".format(filename))
            return True

        else:
            log.error("Doesn't support non-csv or non-gpg uploads yet")
        return False

    def upload_to_remote(self, output_df, filename):
        """
        Method to upload the records to either the Quorum SFTP Server or an external SFTP server

        Args:
            output_df (pd.DataFrame): The Dataframe to upload
            filename (str): The name to call the file uploaded

        Returns:
            bool: Did the upload succeed
        """
        if self.dry_run:
            # Someone could call this method manually from the shell during testing, prevent
            raise ValueError("Currently set to dry run, cannot upload to SFTP server")

        if self.server_type == FlatFileSourceType.quorum_sftp:
            upload_client = QuorumSFTPHelper(folder=self.quorum_sftp_base_path, sftp_user_id=self.quorum_sftp_user_id)
            path = posixpath.join(self.quorum_sftp_base_path, filename)

            # Determine content type based on file extension
            if filename.endswith('.csv.gpg'):
                content_type = b'application/pgp-encrypted'

                # Convert DataFrame to CSV format
                buffer = StringIO()
                output_df.to_csv(buffer, encoding='utf-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
                csv_data = buffer.getvalue().decode('utf-8')

                # Encrypt the CSV data
                encrypted_data = self.encrypt_data(csv_data)
                if encrypted_data is None:
                    log.error("Encryption failed.")
                    return False

                # Convert encrypted data to file-like object
                file_obj = BytesIO(encrypted_data)
            elif filename.endswith('.csv'):
                log.debug("Data types before writing to CSV:{}".format(output_df.dtypes))
                content_type = b'text/csv'
                file_obj = StringIO()
                output_df.to_csv(file_obj, encoding='UTF-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
            else:
                raise NotImplementedError("Does not yet support non-csv files!")

            log.debug("Uploading {} to {} using {} with content type {}".format(file_obj, path, upload_client, content_type))

            # Upload the file
            upload_client._upload_file(file_obj, path, content_type)
            return True
        elif self.server_type == FlatFileSourceType.external_sftp:
            # Determine content type based on file extension
            if filename.endswith('.csv.gpg'):
                content_type = b'application/pgp-encrypted'

                # Convert DataFrame to CSV format
                buffer = StringIO()
                output_df.to_csv(buffer, encoding='utf-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
                csv_data = buffer.getvalue().decode('utf-8')

                # Encrypt the CSV data
                encrypted_data = self.encrypt_data(csv_data)
                if encrypted_data is None:
                    log.error("Encryption failed.")
                    return False

                # Convert encrypted data to file-like object
                file_obj = BytesIO(encrypted_data)
            elif filename.endswith('.csv'):
                log.debug("Data types before writing to CSV:{}".format(output_df.dtypes))
                content_type = b'text/csv'
                file_obj = StringIO()
                output_df.to_csv(file_obj, encoding='UTF-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
            else:
                raise NotImplementedError("Does not yet support non-csv files!")

            log.debug("Uploading {} to external SFTP using {} with content type {}".format(file_obj, filename, content_type))

            # Fetch the credentials from AWS SSM
            external_ftp_creds = get_ssm_param_by_path(param_path=self.aws_ssm_path_to_credentials)
            # Prepare the records
            records = output_df.to_dict('records')

            # Use the helper function
            success = upload_records_to_external_ftp(
                records=records,
                filetype='csv',
                file_name=filename,
                ftp_endpoint=external_ftp_creds['SFTP Endpoint'],
                ftp_username=external_ftp_creds['SFTP Username'],
                ftp_password=external_ftp_creds['SFTP Password'],
                ftp_dir=external_ftp_creds.get('ftp_dir', None),
                overwrite_file=external_ftp_creds.get('overwrite_file', False)
            )

            if success:
                return True
            else:
                raise ValueError("Upload failed. Either the external FTP credentials were incorrect, or there was an issue with the data or file type.")

    def _get_sftp_filepaths_with_change(self, task_name):
        # type: (str) -> List[Tuple[str,bool]]
        """
        Returns a list of [filepath(str), changed(bool)]
        of the most recently changed files from s3, or from a passed in CSV
        Args:
            task_name (str): The name of the task that was passed in
        Returns:
            (List[Tuple[str,bool]])
        """

        task = self.all_tasks[task_name]
        file_pattern = task.sftp_file_name
        path = task.sftp_file_path

        if self.server_type == FlatFileSourceType.quorum_sftp:
            # Folder name is a configuration-level setting
            sftp_folder_name = self.quorum_sftp_base_path

            # File pattern and Delete after Download are task-level parameters
            sftp_delete_downloaded_files = task.sftp_delete_after_download
            return download_and_check_for_update(
                sftp_folder_name,
                s3_file_pattern=file_pattern,
                delete_after_download=sftp_delete_downloaded_files,
                integration_configuration=self.config,
                save_new_checksum=False,    # We don't want to update the checksum until after the sync has run successfully
            )
        else:
            # We are working with some form of external server, either an S3 bucket or an actual SFTP server
            ssm_path = self.aws_ssm_path_to_credentials
            server_credentials = get_ssm_param_by_path(ssm_path)
            if not isinstance(server_credentials, dict):
                quorum_slack_notify(
                    "#ps-integration-errors",
                    "SFTP - integration SSM credentials at path {} are invalid".format(
                        ssm_path
                    ),
                    icon_emoji=":japanese_ogre:"
                )
                raise ValueError

            if self.server_type == FlatFileSourceType.s3_bucket:
                return download_and_check_for_update(
                    path_prefix=path,
                    s3_file_pattern=file_pattern,
                    bucket_name=server_credentials["bucket_name"],
                    aws_access_key_id=server_credentials["aws_access_key_id"],
                    aws_secret_access_key=server_credentials["aws_secret_access_key"],
                    host=server_credentials["aws_region"],
                    delete_after_download=task.sftp_delete_after_download,
                    integration_configuration=self.config,
                    save_new_checksum=False
                )
            elif self.server_type == FlatFileSourceType.external_sftp:
                file_paths = download_files_from_ftp(
                    org_name=self.organization.name,
                    ftp_endpoint=server_credentials["SFTP Endpoint"],
                    ftp_dir=path,
                    ftp_username=server_credentials["SFTP Username"],
                    ftp_password=server_credentials["SFTP Password"],
                    external_identifiers={file_pattern},
                    delete_after_download=task.sftp_delete_after_download,
                )
                return check_files_for_updates(file_paths, integration_configuration=self.config, save_new_checksum=False)
            else:
                raise ValueError("Invalid choice for server.")

    def interation_specific_validate_task(self, task_name):
        # type: (str) -> List[unicode]
        """
        SFTP-specific task validation

        Arguments:
            task_name (str): The name of the task to be validated

        Returns:
            list: List of unicodes describing in plain language the validation failures, if any
        """

        log.info("SFTP has no specific validation required at this time for task {}.".format(task_name))
        pass
