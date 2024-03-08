"""
This houses all of the External-side functionality for integrations with Salesforce
"""
from __future__ import division

# Standard Library
import logging
from datetime import date, datetime
from functools import partial

# Third Party
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceRefusedRequest,
)
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

# Framework
from django.conf import settings
from django.db.models import QuerySet



log = logging.getLogger(__name__)


SF_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
SF_DATE_FORMAT = "%Y-%m-%d"


class NewSalesforceIntegration(NewIntegration):
    """
    New home for the Salesforce Integration
    """
    INTEGRATION_TYPE = IntegrationType.salesforce

    def __init__(
        self,
        config_id,                  # type: int
        dry_run=True,               # type: bool
        force=False,                # type: bool
        dummy_multi=False,          # type: bool
        days_back=None,             # type: Optional[int]
        minutes_back=None,          # type: Optional[int]
        back_to_exact_date=None     # type: Optional[datetime]
    ):
        # type: (...) -> None
        """
        Instantiate a New Integration instance and update the Integration Data Types dictionary for the
        types supported by this integration (will be used for settings validation)

        Args:
            config_id (int): The integration configuration ID to instantiate from
            dry_run (Opt Boolean): Whether to run this as a dry run or not; defaults True to avoid unintended actual runs
            force (bool): For any records that aren't updated due to a comparison between Quorum and External System (e.g. external
                is newer, etc.), override and force the update;
                intended to be used as an argument when running manually in the shell
            dummy_multi (opt Boolean): Whether to actually run multiprocessed or not, defaults False;
                intended to be used as an argument when running manually in the shell
            days_back (opt Int): In searching for data that has been updated since datetime X, set datetime X to this many days before now;
                intended to be used as an argument when running manually in the shell
            minutes_back (opt Int): In searching for data that has been updated since datetime X, set datetime X to this many minutes before now;
                intended to be used as an argument when running manually in the shell
            back_to_exact_date (opt str): Run the integration back to the specific date/time specified in string format;
                intended to be used as an argument when running manually in the shell
        """
        super(NewSalesforceIntegration, self).__init__(
            config_id=config_id,
            dry_run=dry_run,
            force=force,
            dummy_multi=dummy_multi,
            days_back=days_back,
            minutes_back=minutes_back,
            back_to_exact_date=back_to_exact_date
        )

        self.SUPPORTED_QUORUM_OBJECTS.update(
            {
                Bill: SyncAllowedDataDirection.only_export_from_quorum,
                Campaign: SyncAllowedDataDirection.bidirectional,
                Committee: SyncAllowedDataDirection.only_export_from_quorum,
                IssueManagement: SyncAllowedDataDirection.only_export_from_quorum,
                NewStaffer: SyncAllowedDataDirection.only_export_from_quorum,
                Note: SyncAllowedDataDirection.only_export_from_quorum,
                Official: SyncAllowedDataDirection.only_export_from_quorum,
                PressContact: SyncAllowedDataDirection.only_export_from_quorum,
                PublicOrganization: SyncAllowedDataDirection.only_export_from_quorum,
                Regulation: SyncAllowedDataDirection.only_export_from_quorum,
                Supporter: SyncAllowedDataDirection.bidirectional
            }
        )

        if self.dry_run:
            log.error("Salesforce integrations do not yet support dry_run.")

        self.sf_api = SalesforceAPIWrapper(self)
        self.update_external_id_dict()

    def update_external_id_dict(self, quorum_model_list=None):
        # type: (List[str]) -> None
        """
        Update the instance-stored dictionary of external IDs, used to store external IDs
        for record types that do not allow custom fields in Quorum

        Arguments:
            quorum_model_list (opt List(str)): The list of quorum model names to update

        Returns: None
        """
        for model_dict in self.external_id_mapping:
            if quorum_model_list and model_dict["Quorum Model"] not in quorum_model_list:
                continue
            log.debug("Updating for {}".format(model_dict["External Model"]))
            sf_model = model_dict["External Model"]
            quorum_id_field = model_dict["Quorum ID Field"]

            # Retrieve the SF IDs and Quorum IDs for this particular model
            sf_dataframe = self.sf_api.get_all_objects_of_type(
                sf_object_type=sf_model,
                updated_after=None,
                select_fields=[u"Id", quorum_id_field],
                filter_criteria=u"{} != Null".format(quorum_id_field)
            )
            log.info("While updating for {}, received {} records from Salesforce".format(
                model_dict["External Model"],
                len(sf_dataframe)
            ))
            if not sf_dataframe.empty:
                sf_dataframe = sf_dataframe[sf_dataframe[quorum_id_field].notnull()]

                # New code for logging problematic values
                for value in sf_dataframe[quorum_id_field]:
                    try:
                        # Attempt to convert the value to integer
                        int(float(value))
                    except ValueError:
                        # Log the problematic value
                        log.error("Problematic value in '{}': {}".format(quorum_id_field, value))

                # Original conversion with try-except block for logging
                try:
                    sf_dataframe[quorum_id_field] = sf_dataframe[quorum_id_field].astype(int)
                except ValueError as e:
                    log.error("Error converting column '{}' to integers: {}".format(quorum_id_field, e))

                # Proceed with creating the id_dict and updating the external_id_dictionary
                if not sf_dataframe.empty:
                    id_dict = {record[quorum_id_field]: record["Id"] for record in sf_dataframe.to_dict(orient="records")}
                    self.external_id_dictionary[model_dict["Quorum Model"]] = id_dict
                else:
                    self.external_id_dictionary[model_dict["Quorum Model"]] = {}

    def convert_quorum_df_to_external_df(self, task_name, quorum_df):
        # type: (str, pd.DataFrame) -> pd.DataFrame
        """
        Convert a Quorum Dataframe to a Dataframe of data ready to load to Salesforce

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync
            quorum_df (pd.DataFrame): The dataframe to convert from quorum to Salesforce

        Returns:
            dataframe: The converted dataframe
        """

        log.info(u"Begin converting df {} from Quorum to Salesforce for org {}".format(
            task_name,
            self.organization
        ))

        task = self.all_tasks[task_name]

        if task.sf_matching_fields:
            quorum_df = self._check_for_sf_ids_on_new_records(
                quorum_df,
                match_fields=task.sf_matching_fields,
                sf_object_type=task.external_model
            )

        quorum_df = self.clean_df_columns(
            dataframe=quorum_df,
            fields_mapping=task.fields_mapping,
            map_to_quorum=False
        )

        # Run through some Salesforce-specific column additions/changes
        # Task actions should be sent with a "Completed" flag to save correctly
        if task.external_model == "Task" and "status" not in quorum_df.columns:
            quorum_df["Status"] = "Completed"

        quorum_df = self.sf_api.normalize_sf_field_types(dataframe=quorum_df, salesforce_model=task.external_model)

        return quorum_df

    def _check_for_sf_ids_on_new_records(self, dataframe, match_fields, sf_object_type):
        # type: (pd.DataFrame, Dict[str, Union[str, unicode]], str) -> pd.DataFrame
        """
        Query SalesForce for the SF IDs for records that don't already have them,
        so that we can try to match the records in SF against "new" records in Quorum.

        Operates by searching for sets of criteria (to minimize separate API calls) and
        then parsing the dictionary results, updating supporters as it finds them.

        Arguments:
            dataframe (dataframe): The dadtaframe to match on
            match_fields (Dict): The dictionary of fields to match on, in the form {"Quorum Field": "Salesforce Field"}
            sf_object_type (str): The name of the object type in Salesforce

        returns:
            DataFrame: The updated dataframe with the matches added in (if any)
        """
        # Create and filter the matching_df to just be the values we are going to use to match plus the Quorum ID
        # only for records that don't already have a salesforce ID
        # NOTE: This function runs on a Dataframe that has already been converted to Salesforce Field Names
        matching_df_fields = list(match_fields)
        # matching_df_fields.append("quorum_side_primary_key")
        matching_df = dataframe[dataframe["external_unique_id"].isnull()]
        if matching_df.empty:
            log.info("All records already have SF ids.")
            return dataframe
        matching_df = matching_df[matching_df_fields]

        # Don't attempt to match any record where any one of the matching fields is None
        for field_name in match_fields:
            matching_df = matching_df[matching_df[field_name].notnull()]
            matching_df = matching_df[matching_df[field_name] != ""]
            if matching_df.empty:
                log.info("No records available to match.")
                return dataframe

        search_values = matching_df.to_dict(orient="records")
        sf_fields_for_match = listvalues(match_fields)
        sf_fields_for_match.append("Id")
        all_records_df = self.sf_api.query_sf(
            search_values=search_values,
            field_mapping=match_fields,
            object_type=sf_object_type,
            select_fields=sf_fields_for_match,
            include_deleted=False
        )

        if all_records_df.empty:
            log.info("No matching records returned from Salesforce.")
            return dataframe

        # Ensure column types match so that merging works (merge cannot join on columns of different types)
        for column_name in match_fields:
            matching_df[column_name] = matching_df[column_name].astype(dataframe[column_name].dtype)

        all_records_df = all_records_df.rename(columns={"Id": "external_unique_id"})
        dataframe = dataframe.merge(
            all_records_df,
            how="left",
            on=list(match_fields),
            suffixes=("", "_matching"),
            validate="one_to_one"
        )

        # Now, where there isn't an external_unique_id already, replace with the one we found while matching,
        # Then get rid of the matching column
        def _replace_if_none(row):
            if row["external_unique_id"]:
                return row["external_unique_id"]
            return row["external_unique_id_matching"]

        dataframe["external_unique_id"] = dataframe.apply(_replace_if_none, axis=1)
        dataframe.drop(columns=["external_unique_id_matching"], inplace=True)

        return dataframe

    def send_external_df_to_external(self, task_name, external_df):
        # type: (str, pd.DataFrame) -> bool
        """
        Stub method that sends a Dataframe of Quorum-originated data to Salesforce

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync
            external_df (pd.DataFrame): The dataframe to send to Salesforce

        Returns:
            bool: Was the send to external successful or not?
        """
        task = self.all_tasks[task_name]
        max_batch_size = task.sf_max_batch_size or self.sf_api.SF_API_MAX_BATCH_SIZE
        store_sf_ids_in_quourm = task.sf_store_ids_in_quorum

        send_df = external_df.where(pd.notnull(external_df), None)

        success, results = self.sf_api.run_upsert_to_sf(
            salesforce_model=task.external_model,
            send_df=send_df,
            max_batch_size=max_batch_size,
            match_on=task.sf_match_on,
        )

        if success:
            if not store_sf_ids_in_quourm:
                log.info("Task run completed successfully for {}, but is set not to store SF IDs in Quorum.".format(task_name))
                return True

            # We actually have a result and are meant to store the IDs in Quorum,
            # so identify if any records have new IDs and send those to Quorum
            results_df = pd.DataFrame.from_records(results)
            id_update_df = results_df[results_df["quorum_id"].notna() & results_df["sf_id"].notna()]
            if id_update_df.empty:
                log.info("No IDs to update for {}".format(task_name))
                return True
            id_update_df = id_update_df[["quorum_id", "sf_id"]]
            id_update_df = id_update_df.rename(columns={"sf_id": "external_unique_id"})

            # Now, depending on where we store custom IDs, send it the right place
            if self.external_id_custom_slugs_dict.get(task.quorum_model._meta.object_name):
                # We are using the ID Dictionary, just refresh it for this type of model
                self.quorum_side_helper.send_ext_ids_to_quorum(task_name=task_name, id_df=id_update_df)
            elif task.quorum_model in self.external_id_dictionary:
                # This is a model that uses the external_id_dictionary, update it
                self.update_external_id_dict(quorum_model_list=[task.quorum_model])

            else:
                # Send the dataframe to Slack so we don't lose it forever
                notify_prof_svcs(
                    is_error=True,
                    message="On {}, nowhere to store external IDs for {}".format(self, task_name),
                    dataframe_to_attach_as_file=id_update_df
                )
                raise ValueError("Nowhere to save incoming external IDs!")

            return True
        else:
            notify_prof_svcs(
                is_error=True,
                message="Upsert to SF failed entirely for task {} on {}".format(task_name, self),
                text_to_attach_as_file="Results dictionary was:\n{}".format(results),
            )
            return False

    def get_external_df_from_external(self, task_name):
        """
        Stub method to retrieve data from Salesforce, returning as a dataframe.

        Args:
            task_name (str): The task name (as specified in the Configuration) to get.

        Returns:
            dataframe: The external data, loaded into a DataFrame.
        """
        # Get the task configuration object based on the task_name
        if task_name not in self.all_tasks:
            raise ValueError("Task configuration not found for task {}".format(task_name))
        task_config = self.all_tasks[task_name]

        # Update instance variables for object_type, select_fields, and field_mapping
        current_object_type = task_config.external_model
        current_select_fields = [mapping[0] for mapping in task_config.fields_mapping.values()]
        current_field_mapping = task_config.fields_mapping.copy()

        # Extend search_values based on provided filters
        search_values = {}
        filters = task_config.external_filter  # Assuming this is a dictionary
        filter_mapping = {}
        for filter_key, filter_value in filters.items():
            if filter_key == "updated_after" and filter_value == "Last Successful Run":
                filter_key = "LastModifiedDate"
                filter_value = self.get_how_far_to_go_back()
            search_values[filter_key] = filter_value
            filter_mapping[filter_key] = filter_key

        # Use the updated search_values to build the WHERE clause
        where_clauses = self.sf_api._sf_build_where_clauses([search_values], filter_mapping)

        log.info("Constructed WHERE clause: {} ".format(where_clauses))
        current_field_mapping = {value[1]: value[1] for value in current_field_mapping.values()}
        # Execute the query using Salesforce's query_all method
        external_df = self.sf_api.query_sf(
            object_type=current_object_type,
            select_fields=current_select_fields,
            field_mapping=current_field_mapping,
            where_clauses=where_clauses,  # Use the constructed WHERE clause
            async_queries=True
        )

        return external_df

    def convert_ext_df_to_quorum_df(self, task_name, external_df):
        """
        Converts and preprocesses a Salesforce dataframe for import into Quorum.

        This function performs the following steps:
        1. Fetches the task configuration using the task name.
        2. Applies any literal fields to the dataframe.
        3. Applies a list of preprocessors to the dataframe if specified in the task configuration.
        4. Cleans up the dataframe columns based on the task's fields mapping.

        Args:
            task_name (str): The name of the task as specified in the Configuration, used for syncing.
            external_df (pd.DataFrame): The dataframe retrieved from Salesforce.

        Returns:
            pd.DataFrame: The dataframe after applying all transformations, ready for import into Quorum.
        """
        task = self.all_tasks[task_name]
        fields_mapping = task.fields_mapping

        preprocessors_list = task.external_processors

        log.info(u"Running preformatters (if any) for {}, integration configuration {}".format(self.organization, self.config.id))

        # Add the literal fields, if any
        external_df = self.apply_literal_fields(dataframe=external_df, is_quorum_import=True, task_name=task_name)

        if preprocessors_list:
            for preprocessor_name, argument in preprocessors_list:
                preprocessor_class = get_salesforce_external_preprocessor(preprocessor_name)
                if preprocessor_class:
                    preprocessor = preprocessor_class(self)
                    log.info(u"Applying preprocessor {} for task {}".format(preprocessor_name, task_name))
                    external_df = preprocessor.apply(external_df, argument)

                if external_df.empty:
                    log.info(u"Dataframe has been emptied by {} on {}!".format(preprocessor_name, argument))
                    return external_df

        external_df = self.clean_df_columns(dataframe=external_df, fields_mapping=fields_mapping, map_to_quorum=True)

        return external_df

    def convert_field_mapping_to_query(self, task_name):
        """
        Convert the task's field mapping to a Salesforce SOQL query.

        Args:
            task_name (str): The name of the task for which to generate the query.

        Returns:
            tuple: A tuple containing:
                - The constructed SOQL query (str).
                - The Salesforce object type (str).
                - The selected fields for the query (list of str).
                - The list of WHERE clauses for the query (list of str).

        Example:
            Assuming a task "contact_sync" is set up to fetch all contacts modified after
            the last successful run from Salesforce. If the last successful run was on
            2023-01-01 and the task has a field mapping for "Name" and "Email",
            this method might return:

            ("SELECT Name, Email FROM Contact WHERE LastModifiedDate >= '2023-01-01'",
            "Contact", ["Name", "Email"], ["LastModifiedDate >= '2023-01-01'"])
        """

        # Get the task configuration object based on the task_name
        if task_name not in self.all_tasks:
            raise ValueError("Task configuration not found for task {}".format(task_name))
        task_config = self.all_tasks[task_name]

        # Extract the field mapping from the task configuration
        field_mapping = task_config.fields_mapping

        # Extract the object type and select fields based on the field mapping
        object_type = task_config.external_model
        select_fields = [mapping[0] for mapping in field_mapping.values()]  # Get the external field names

        # New date filter logic
        last_successful_run_date = self.config.date_of_last_successful_run
        where_clause_list = []

        # Check for updated_after in task_config.external_filter
        if "updated_after" in task_config.external_filter:
            if task_config.external_filter["updated_after"] == "Last Successful Run" and last_successful_run_date:
                date_filter = last_successful_run_date.strftime(SF_DATETIME_FORMAT)
                where_clause_list.append("LastModifiedDate >= '{}'".format(date_filter))
            else:
                where_clause_list.append("LastModifiedDate >= '{}'".format(task_config.external_filter["updated_after"]))

        if task_config.external_filter:
            for key, value in task_config.external_filter.items():
                # Check for modifiers and set the operator accordingly
                if key.endswith("__gte"):
                    operator = ">="
                    key = key[:-5]  # remove "__gte" from the end of the key
                elif key.endswith("__gt"):
                    operator = ">"
                    key = key[:-4]  # remove "__gt" from the end of the key
                elif key.endswith("__lte"):
                    operator = "<="
                    key = key[:-5]  # remove "__lte" from the end of the key
                elif key.endswith("__lt"):
                    operator = "<"
                    key = key[:-4]  # remove "__lt" from the end of the key
                else:
                    operator = "="

                where_clause_list.append("{}{}'{}'".format(key, operator, value))

        # Constructing the query
        query = "SELECT {} FROM {}".format(",".join(select_fields), object_type)
        if where_clause_list:
            query += " WHERE {}".format(" AND ".join(where_clause_list))

        return query, object_type, select_fields, where_clause_list

    def _get_salesforce_queries_for_task(self, task_name):
        """
        Retrieves the Salesforce queries for a specific task based on the task configuration.

        Args:
            task_name (str): The name of the task for which to generate the queries.

        Returns:
            list[dict]: A list of dictionaries, each containing the generated SOQL query, object type, external model,
            field mapping, and where_clause_str for the task. Returns an empty list if the task configuration is not found.
        """
        # Get the task configuration object based on the task_name
        if task_name not in self.all_tasks:
            raise ValueError("Task configuration not found for task {}".format(task_name))
        task_config = self.all_tasks[task_name]

        # Extracting the external model and field mapping from the task configuration
        external_model = task_config.external_model

        # Generating the Salesforce query for the task
        query, object_type, select_fields, where_clause_str = self.convert_field_mapping_to_query(task_name)

        # Return the queries as a list (in case you want to support multiple queries for a task in the future)
        return [
            {
                "query": query,
                "object_type": object_type,
                "select_fields": select_fields,
                "external_model": external_model,
                "field_mapping": task_config.fields_mapping,
                "task_name": task_name,
                "where_clause_str": where_clause_str  # Include where_clause_str in the returned dictionary
            }
        ]

    def run_task_from_external_crm_to_quorum(self, task_name, filters=None):
        """
        Stub function that synchronizes data from Salesforce to Quorum

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync
            filters (dict): Additional filters to apply when fetching data from external service
        Returns:
            list[dict]: A results list of records processed, the outcome, and any additional metadata, in the form:
                [
                    {
                        "id": "Quorum ID",
                        "external_id": "External ID, if provided",
                        "outcome": "one of Error, Updated, Created, tbd",
                        "metadata": "Whatever else is relevant, e.g. the error message",
                    }, ...
                ]
        """
        log.info(u"Running task {} from Salesforce to Quorum.".format(task_name))
        # New logic to check task_config for external_filter or use default date filter
        task_config = self.all_tasks[task_name]
        last_successful_run_date = self.config.date_of_last_successful_run
        if last_successful_run_date:
            date_filter = last_successful_run_date.strftime(SF_DATETIME_FORMAT)
            filters = {"LastModifiedDate": date_filter}
        else:
            filters = {}

        if task_config and task_config.external_filter:
            # Check if updated_after exists in external_filter
            if "updated_after" in task_config.external_filter and task_config.external_filter["updated_after"] == "Last Successful Run":
                # Replace updated_after with LastModifiedDate
                filters["LastModifiedDate"] = filters["LastModifiedDate"]

        # Determine the Salesforce queries or endpoints based on the task_name
        salesforce_data_queries = self._get_salesforce_queries_for_task(task_name=task_name)
        if not salesforce_data_queries:
            raise ValueError("No Salesforce queries found for task '{}'. Ensure the task is correctly configured.".format(task_name))

        results = []
        some_change = False
        for query_info in salesforce_data_queries:

            # Retrieve the raw data from Salesforce using named parameters
            external_df = self.get_external_df_from_external(task_name=query_info["task_name"])

            # Convert the Salesforce data into a Quorum DataFrame
            converted_df = self.convert_ext_df_to_quorum_df(task_name=task_name, external_df=external_df)

            if self.dry_run:
                quorum_headers = self.quorum_side_helper._make_quorum_headers(task=self.all_tasks[task_name])
                results.append({"Quorum Headers": quorum_headers, "Dataframe": converted_df})
            else:
                # Send the Quorum DataFrame into Quorum
                result = self.quorum_side_helper.send_df_to_quorum(task_name=task_name, quorum_df=converted_df)
                results.append(result)

        # Check if results is not empty or None
        some_change = bool(results)

        if not some_change:
            log.warning(u"No changes made for task {}".format(task_name))

        if self.dry_run:
            return {"dry_run_results": results}

        # In a live run, determine success and potential errors
        success = all(result is True for result in results)
        errors = [error for error in results if error is not True]

        if success:
            return True
        else:
            return {
                "success": False,
                "errors": errors,
                "some_change": some_change  # Returning the flag could be useful
            }

    def run_task_from_quorum_to_external_crm(self, task_name):
        # type: (str) -> Union[pd.DataFrame, bool]
        """
        Sub function that will someday run a task from Salesforce to Quorum

        Args:
            task_name (str): The task name (as specified in the Configuration) to sync

        Returns:
            DataFrame or Bool:
                DataFrame: IFF dry run, returns the Dataframe produced by the dry run
                Bool: IFF NOT dry run, returns whether the task succeeded in its entirety
        """
        log.info(u"Running task {} from Quorum to Salesforce.".format(task_name))

        quorum_df = self.quorum_side_helper.get_df_from_quorum(task_name=task_name)

        if not isinstance(quorum_df, pd.DataFrame) or quorum_df.empty:
            # Didn't find anything to send
            return None

        salesforce_df = self.convert_quorum_df_to_external_df(task_name, quorum_df)

        if salesforce_df.empty:
            return None

        if self.dry_run:
            log.info("Would send {} rows for task {}".format(len(salesforce_df), task_name))
            return salesforce_df
        else:
            try:
                return self.send_external_df_to_external(task_name=task_name, external_df=salesforce_df)
            except Exception as e:
                log.error("Error occurred sending to SF: {}".format(e), exc_info=True)

    def interation_specific_validate_task(self, task_name):
        # type: (str) -> List[unicode]
        """
        Salesforce-specific task validation

        Arguments:
            task_name (str): The name of the task to be validated

        Returns:
            list: List of unicodes describing in plain language the validation failures, if any
        """

        log.info("Salesforce has no specific validation required at this time for task {}.".format(task_name))
        pass


class SalesforceAPIWrapper(object):
    """
    Wrapper for Salesforce-interaction functions (for organization purposes only)
    """
    # Maximum Number of Records for Bulk API Limit
    # Source: https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm
    SF_API_MAX_BATCH_SIZE = 10000
    MAX_WHERE_CLAUSE = 3500    # SF Limits the allowed length of WHERE queries

    def __init__(self, processor_instance):
        self.processor_instance = processor_instance
        self.organization = processor_instance.organization
        if not settings.TESTS_IN_PROGRESS:
            self.sf = self.processor_instance.config.connect_to_salesforce()
        else:
            self.sf = None

    @staticmethod
    def _should_ignore_error(salesforce_model, response):
        # type: (str, dict) -> bool
        """Returns whether a given error is expected, and should thus be ignored"""

        if salesforce_model == "CampaignMember":
            # This error is thrown if we attempt to update an existing CampaignMember
            if response["errors"][0].get("statusCode", "") == "INVALID_FIELD_FOR_INSERT_UPDATE":
                return True
        return False

    def get_all_objects_of_type(
        self,
        sf_object_type,                     # type: str
        select_fields=[u"Id"],               # type: List[Union[str, unicode]]
        updated_after=None,                 # type: Optional[datetime]
        filter_criteria="",                 # type: Union[str, unicode]
        include_deleted=True                # type: bool
    ):
        # type: (...) -> pd.DataFrame
        """
        Retrieve all relevant Salesforce Objects of the given object_type in the Salesforce instance

        Arguments:
            sf_object_type (str): The type of object to query, for example "ContentNote"
            select_fields (list): The fields that we actually want to retrieve for the objects
            updated_after (opt. datetime): Only retrieve objects updated after a given date
            filter_criteria (str): A Salesforce "where" clause to additionally filter the data
            include_deleted (bool): Whether or not to include deleted records

        Returns:
            list(dict): A list of results, either single values if only one select_field provided, or a list of dictionaries if multiple
        """

        date_criteria = u""
        if updated_after:
            # This is the format that Salesforce expects.
            # An example would be "07/10/2018 20:41:44 UTC" => "2018-07-10T20:41:44+0000"
            date_criteria = u"LastModifiedDate >= {}".format(updated_after.strftime(SF_DATETIME_FORMAT))

        if updated_after or filter_criteria:
            where_clauses = [u" AND ".join([_f for _f in [date_criteria, filter_criteria] if _f])]
        else:
            where_clauses = []

        records = self.query_sf(
            object_type=sf_object_type,
            select_fields=select_fields,
            where_clauses=where_clauses,
            include_deleted=include_deleted
        )
        return records

    def query_sf(
        self,
        object_type,                    # type: str
        select_fields,                  # type: List[Union[str, unicode]]
        search_values=None,             # type: Optional[Dict]
        field_mapping=None,             # type: Optional[Dict]
        where_clauses=None,             # type: Optional[List[Union[str, unicode]]]
        include_deleted=False,          # type: bool
        async_queries=False,            # type: bool
    ):
        # type: (...) -> pd.DataFrame
        """
        Accepting either a pre-built where clause OR the search values and field mappings to
        create where clauses, actually run the query in Salesforce, collecting responses and
        aggregating them into a single output

        Args:
            object_type (str): The Salesforce object type (e.g. "Contact" or "Account")
            select_fields (List[str]): The salesforce fields we actually want to retrieve
            search_values (Opt Dict): If not providing a prebuilt where_clauses, the dictionary of values to
                query for in salesforce
            field_mapping (Opt Dict): If not providing a prebuilt where_clauses, the dictionary of mappings from
                the keys in the dictionary to the salesforce field names
            where_clauses (Optlist[str]): If not providing search values / fields mapping this is the list of string
                Where clauses prebuilt for the query to SF - note these must be properly formatted for direct
                submission to Salesforce
            include_deleted (Opt bool): whether to include deleted records in the return from salesforce

        returns:
            dataframe: The dataframe of responses received from SF, comprising all of the matched values and the SF id
        """

        if where_clauses is None and not (search_values and field_mapping):
            raise ValueError("Must supply either an explicit where clause or both search_values and fields_mapping.")

        if where_clauses is None:
            where_clauses = self._sf_build_where_clauses(search_values=search_values, field_mapping=field_mapping)

        all_records = []
        count = 0
        if async_queries:
            def fetch_records(sf_where):
                return self._run_sf_query(
                    where_clause=sf_where,
                    object_type=object_type,
                    select_fields=select_fields,
                    include_deleted=include_deleted
                )

            all_records = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_sf_where = {executor.submit(fetch_records, sf_where): sf_where for sf_where in where_clauses}
                for future in as_completed(future_to_sf_where):
                    sf_where = future_to_sf_where[future]
                    try:
                        records = future.result()
                    except Exception as exc:
                        log.debug('{} generated an exception: {}'.format(sf_where, exc))
                    else:
                        log.info(u"SF returned {} new records".format(len(records)))
                        if records:
                            all_records.extend(records)
        else:
            # existing synchronous code
            for sf_where in where_clauses:
                count += 1
                log.info(u"Running query {} of {} in Sf.".format(count, len(where_clauses)))
                records = self._run_sf_query(
                    where_clause=sf_where,
                    object_type=object_type,
                    select_fields=select_fields,
                    include_deleted=include_deleted
                )
                log.info(u"SF returned {} new records".format(len(records)))
                if records:
                    all_records.extend(records)

        if not all_records:
            return pd.DataFrame()

        dataframe = pd.DataFrame.from_records(all_records)
        dataframe = dataframe[select_fields]
        if field_mapping:
            dataframe = dataframe.rename(columns={value: key for key, value in field_mapping.items()})

        return dataframe

    def _run_sf_query(self, where_clause, object_type, select_fields, include_deleted=True):
        # type: (Union[str, unicode], Union[str, unicode], List[Union[str, unicode]], Optional[bool]) -> List
        """
        Retrieve all relevant SObjects of the given object_type in the Salesforce instance using the specified
        WHERE criteria, returning the select_fields specified

        Do note that for a datetime filter, the Salesforce API does require UTC.

        Arguments:
            where_clause: The WHERE to search on SF for
            object_type (str): The type of object in SF being searched for
            select_fields (list): The list of fields to get for the object

        Returns: a list of dictionaries with the SF select_fields returned by SF
        """

        # Construct the SOQL query and execute it
        if where_clause:
            query = u"SELECT {} FROM {} WHERE {} ORDER BY Id".format(",".join(select_fields), object_type, where_clause)
        else:
            query = u"SELECT {} FROM {} ORDER BY Id".format(",".join(select_fields), object_type)

        log.info(u"The SOQL query being executed for {} is {}".format(
            self.processor_instance.organization,
            query
        ))

        try:
            if settings.TESTS_IN_PROGRESS:
                records = []
            else:
                records = self.sf.query_all(query, include_deleted=include_deleted)["records"]

        except SalesforceRefusedRequest as e:
            try:
                error_code = e.content[0]["errorCode"]
                if error_code == "INVALID_OPERATION_WITH_EXPIRED_PASSWORD":
                    notify_prof_svcs(
                        is_error=True,
                        message="SF: Integration {} has an invalid/expired password.".format(self),
                    )
                    raise e
                else:
                    notify_prof_svcs(
                        is_error=True,
                        message="SF: For integration {}, SF refused the request with an error other than expired password.".format(self),
                        threaded_content="Refusal error: {}\nError dictionary: {}".format(e, e.content)
                    )
                    raise e
            except Exception as second_e:
                notify_prof_svcs(
                    is_error=True,
                    message="SF: For integration {}, SF refused the request with an error other than expired password.".format(self),
                    threaded_content="Refusal error: {}\nError getting error: {}".format(e, second_e)
                )
                raise e

        return records

    def _sf_build_where_clauses(self, search_values, field_mapping):
        # type: (QuerySet, Dict[str, str]) -> List[unicode]
        """
        For a given list of search values (as dictionaries) and a dictionary of field mappings, assemble
        the relevant Salesforce Where Clauses to search for them.

        Args:
            search_values (List[Dict]): A list of dictionaries for the values we are searching for
            field_mapping (Dict): The alignment of the search_values keys to SF fields

        returns:
            list[str]: A list of string Where clauses to use in a query to Salesforce
        """
        current_where = u''
        sf_where_clauses = []
        log.info(u"Preparing query for {} records".format(len(search_values)))
        for idx, record in enumerate(search_values):
            if len(current_where) > self.MAX_WHERE_CLAUSE:
                sf_where_clauses.append(current_where)
                log.info(u"Added where clause {}".format(len(sf_where_clauses)))
                current_where = u''

            where_items = []

            # Iterate the keys in alphabetical order, so that ordering is deterministic for testing purposes
            for value_key, sf_name in sorted(listitems(field_mapping)):
                value_to_search_for = record.get(value_key)

                if value_to_search_for is not None:
                    if isinstance(value_to_search_for, bool):
                        # Format boolean values without quotes
                        where_items.append(u"{} = {}".format(sf_name, 'TRUE' if value_to_search_for else 'FALSE'))
                    elif isinstance(value_to_search_for, (datetime, date)):
                        # Format datetime or date values for Salesforce
                        formatted_date = value_to_search_for.strftime(SF_DATETIME_FORMAT)
                        if sf_name == 'LastModifiedDate':
                            # Special handling for 'LastModifiedDate'
                            where_items.append(u"{} >= {}".format(sf_name, formatted_date))
                        else:
                            where_items.append(u"{} = '{}'".format(sf_name, formatted_date))
                    elif isinstance(value_to_search_for, (int, float)):
                        # Numeric fields should not be in quotes
                        where_items.append(u"{} = {}".format(sf_name, value_to_search_for))
                    else:
                        # Other types (assuming string) should be in quotes
                        value_to_search_for = unicode(value_to_search_for).replace("'", "\\'")
                        where_items.append(u"{} = '{}'".format(sf_name, value_to_search_for))

            if not where_items:
                continue
            current_clause = u"({})".format(" AND ".join(where_items))

            if current_where:
                # Need to join the current where every time, otherwise the length limit can't be applied
                current_where = " OR ".join([current_where, current_clause])
            else:
                current_where = current_clause

            # Added condition to check if it's the last record
            if len(current_where) > self.MAX_WHERE_CLAUSE or idx == len(search_values) - 1:
                sf_where_clauses.append(current_where)
                current_where = u''

        return sf_where_clauses

    def run_upsert_to_sf(self, salesforce_model, send_df, max_batch_size=SF_API_MAX_BATCH_SIZE, match_on="Id"):
        # type: (str, pd.DataFrame, Optional[int], str) -> Tuple[bool, List[Dict[str, Any]]]
        """
        Actually run an upsert to Salesforce for the dataframe (already mapped to SF fields) in batches, matching on
        the provided argument or Id, if none provided

        Arguments:
            salesforce_model (str): The API name of the salesforce model, e.g. "Contact" or "Bill__c"
            send_df (dataframe): The dataframe to send, with column names matching Salesforce Fields on the selected model
            max_batch_size (int): The maximum number of records in one batch (some SF instances have limits)
            match_on (str): What salesforce field to match incoming records to existing records using (e.g. "Id" or "Quorum_ID__c")

        Returns:
            Tuple(bool, results):
                bool: Did the upsert succeed completely
                results: The list of dictionaries of results that were completed, if any
        """
        if not match_on:
            match_on = "Id"
        external_list = send_df.to_dict(orient="records")
        send_df = None
        total_results = []
        overall_success = False
        try:
            # Run upsert in groups not exceeding the max batch size due to Salesforce's Bulk API limit, and
            # provide logging to indicate current completion rate
            num_chunks = int(len(external_list) / max_batch_size) + 1
            current_chunk = 1
            for to_upsert_chunked in chunked_list(
                external_list,
                max_batch_size,
                len(external_list)
            ):
                log.info(u"Processing chunk {} of {}".format(current_chunk, num_chunks))
                current_chunk += 1
                total_results.extend(self._run_upsert_chunk(
                    salesforce_model=salesforce_model,
                    to_upsert_chunked=to_upsert_chunked,
                    match_on=match_on
                ))
            overall_success = True
        except (SalesforceMalformedRequest, requests.exceptions.ConnectionError) as e:
            # If this error occurs at all, we want to stop the run and return (rather than continuing to try)
            # self.monitor.increment_stat("crm.error_count")
            error_message = "Could not upsert Salesforce {} for org {}. Error:\n{}".format(
                salesforce_model,
                self.organization,
                e
            )
            notify_prof_svcs(
                is_error=True,
                message=error_message,
            )
            log.error(error_message)

        return overall_success, total_results

    def _upsert_bulk_wrapper(self, salesforce_model, to_upsert_chunked, match_on="Id"):
        # type: (str, List[Dict], str) -> List[Dict[str, Any]]
        """
        Wrap the actual upsert function so that it is easily mocked for testing

        Arguments:
            salesforce_model: The name of the salesforce model
            to_upsert_chunked: The List of Dicts to upsert,
            match_on: The field to match records to existing records in salesforce

        Returns:
            List[Dict[str, any]]: The results
        """
        return getattr(self.sf.bulk, salesforce_model).upsert(to_upsert_chunked, match_on)

    def _run_upsert_chunk(self, salesforce_model, to_upsert_chunked, match_on="Id"):
        # type: (str, List[Dict], str) -> List[Dict[str, Any]]
        """
        Upsert a set of Salesforce results, record the responses in the Database
        Run this chunk-by-chunk so that if there is an error, we don't corrupt correctly uploaded results

        Args:
            salesforce_model: The Salesforce model name we are sending in
            to_upsert_chunked: The chunk of records we are sending in, including the quorum_id

        Returns:
            list: Output results in the form:
                result = {"success": <TRUE/FALSE>, "quorum_id": <QUORUM_ID>, "sf_id": <SALESFORCE_ID>}

        """
        # Split out the dicts in the upsert_chunk to a list of quorum IDs and a list of data to actually send to SF
        quorum_id_list = []
        for record in to_upsert_chunked:
            quorum_id_list.append(record.pop("quorum_side_primary_key", None))

        # Actually run the Upsert, depending on whether it is a one-at-a-time or a multi-upsert
        if salesforce_model == "ContentNote":
            # ContentNote cannot be bulk upserted by simple_salesforce (library limitation)
            upsert_result_list = self._send_records_one_at_a_time(
                salesforce_model=salesforce_model,
                external_list=to_upsert_chunked,
                match_on=match_on
            )
        else:
            upsert_result_list = self._upsert_bulk_wrapper(
                salesforce_model=salesforce_model,
                to_upsert_chunked=to_upsert_chunked,
                match_on=match_on
            )

        if len(quorum_id_list) != len(upsert_result_list):
            # self.monitor.increment_stat("crm.error_count")
            log.error(u"Salesforce upsert result for org {} ({}) does not match the number of {} to upsert ({})".format(
                self.organization,
                len(upsert_result_list),
                salesforce_model,
                len(quorum_id_list)
            ))

        # Aggregate the upserts where success is False, for more visibility
        # upsert_result_list will be a list of dicts that looks like
        # FAILED RECORD:
        # [{
        #   u'created': False,
        #   u'errors': [{
        #       u'extendedErrorDetails': None,
        #       u'fields': [u'BillingState'],
        #       u'message': u"Please enter the State's abbreviation (ie. IL instead of Illinois)",
        #       u'statusCode': u'FIELD_CUSTOM_VALIDATION_EXCEPTION'}
        #   ],
        #   u'id': None,
        #   u'success': False
        # }]
        #
        # SUCCEEDED RECORD:
        # [{
        #   u'created': False,
        #   u'errors': [],
        #   u'id': u'0010v00000I0L5FAAV',
        #   u'success': True
        # }]

        output_results = []
        for one_result in zip(quorum_id_list, upsert_result_list):
            try:
                quorum_id, obj_upsert_result = one_result
                sf_id = obj_upsert_result.get("id")
                if obj_upsert_result["success"] and obj_upsert_result["id"]:
                    # Update Metrics
                    if obj_upsert_result.get("created"):
                        # self.monitor.increment_stat_by_model("crm.{}.created_count", salesforce_model)
                        log_type = u"Created"
                    else:
                        # self.monitor.increment_stat_by_model("crm.{}.updated_count", salesforce_model)
                        log_type = "Updated"

                    log.debug(u"{} {} {} for {} in Salesforce".format(
                        log_type,
                        salesforce_model,
                        obj_upsert_result["id"],
                        self.organization
                    ))
                    result = {"success": True, "quorum_id": quorum_id, "sf_id": obj_upsert_result["id"]}
                    output_results.append(result)
                else:
                    # Something wrong going on with the upsert
                    # self.monitor.increment_stat_by_model("crm.{}.error_count", salesforce_model)
                    if not self._should_ignore_error(salesforce_model, obj_upsert_result):
                        # Some errors, like duplicate errors on certain datasets, we can safely ignore; it still counts as a
                        # failure but doesn't need to be logged in the error logger / saved to the object as an error

                        error_message = obj_upsert_result.get("message") or obj_upsert_result.get("errors")
                        log.error(u"Unable to upsert {} (Quorum ID {}, SF ID {}) for org {}. The error message was: {}".format(
                            salesforce_model,
                            quorum_id,
                            sf_id,
                            self.organization,
                            error_message
                        ))
                    result = {"success": False, "quorum_id": quorum_id, "sf_id": None}

                    output_results.append(result)
            except Exception as e:
                log.error("Unable to record result - {}.".format(e), exc_info=True)
                result = {"success": False, "quorum_id": quorum_id, "sf_id": None}

                output_results.append(result)

        return output_results

    def _send_records_one_at_a_time(self, salesforce_model, external_list, match_on="Id"):
        # type: (str, List[Dict], str) -> List[Dict[str, Any]]
        """
        Send records for models that don't support batch update one-at-a-time

        Arguments:
            salesforce_model: The name of the SF Model to upsert
            external_list: The list of records to upsert
            match_on: The field to use to uniquely identify existing records to SF

        Returns:
            Dict: A dictionary of results with the following keys:
                id: the salesforce ID
                success: boolean, whether the record succeeded
                errors: The list of errors experienced on the record, if any
                created: boolean, whether the record was created or not
        """
        # For reasons not yet identified, Bulk SF Upsert doesn't work with ContentNotes, so have to do it "by hand"
        upsert_result_list = []
        for record in external_list:
            try:
                # Pop out the SF ID (which can never be in the body) and, if applicable, the match_on variable
                # If no match_on varoable, set match_on to sf_id
                sf_id = record.pop("Id")
                if match_on != "Id":
                    match_value = record.pop(match_on)
                    if not match_value:
                        upsert_result_list.append({
                            "id": sf_id,
                            "success": False,
                            "errors": ["No match_on value provided for record {}.".format(record)],
                            "created": False}
                        )
                        continue
                else:
                    match_value = sf_id

                if match_value:
                    # If we have a SF ID already, udpate the record
                    # The simple_salesforce update method responds with an HTML status code
                    response_code = getattr(self.sf, salesforce_model).update("{}/{}".format(match_on, match_value), record)
                    created = True
                    if isinstance(response_code, int) and response_code == requests.codes.no_content:
                        outcome = {
                            "id": sf_id,
                            "success": True,
                            "errors": [],
                            "created": False
                        }
                else:
                    # The simple_salesforce create method responds with a dictionary of the results if successful
                    outcome = self.sf.ContentNote.create(record)
                    created = False

                if isinstance(outcome, Dict):
                    upsert_result_list.append({
                        "id": outcome.get("id"),
                        "success": outcome.get("success"),
                        "errors": [],
                        "created": created}
                    )
                else:
                    upsert_result_list.append({
                        "id": None,
                        "success": False,
                        "errors": ["Returned a nondictionary result of type {}, being {}".format(type(outcome), outcome)],
                        "created": created}
                    )
            except Exception as e:
                upsert_result_list.append({
                    "id": "None",
                    "success": False,
                    "errors": e,
                    "created": False}
                )
        return upsert_result_list

    def _get_sf_field_definition(self, salesforce_model):
        # type: (str) -> Dict
        """
        Get the SF Field definition for a given Salesforce model; separately defined
        to facilitate mocking during testing

        Arguments:
            salesforce_model: the name of the salesforce model

        Returns:
            OrderedDict: the definition of the field, as returned by SF
        """
        sf_model_obj_ref = getattr(self.sf, salesforce_model)
        description = sf_model_obj_ref.describe()
        return description

    def get_sf_field_definition_dict(self, salesforce_model):
        # type: (str) -> Dict[str, Any]
        """
        Retrieve the field definition parameters from the Salesforce instance for
        the specified model

        Arguments:
            salesforce_model(str): The Name of the model in salesforce

        Returns:
            dict: A dictionary in the form of {"Name": {
                    "Is External ID": True,
                    "Label": "Name"
                    "Length": 80,
                    "Pick List": ["value", "value"],
                    "Required": True,
                    "Type": "String",
                }}
                specifying the field parameters in Salesforce
        """

        description = self._get_sf_field_definition(salesforce_model)
        field_def_dict = {}
        for model_field in description["fields"]:
            field_def_dict[model_field["name"]] = {
                "Is External ID": model_field["externalId"],
                "Label": model_field["label"],
                "Name": model_field["name"],
                "Required": not model_field["nillable"],
                "Type": model_field["type"],
                "Length": model_field["length"],
            }
            if model_field["type"] == "picklist":
                picklist = []
                for picklist_value in model_field["picklistValues"]:
                    if picklist_value["active"]:
                        picklist.append(picklist_value["value"])
                field_def_dict[model_field["name"]]["Pick List"] = picklist
            else:
                field_def_dict[model_field["name"]]["Pick List"] = None

        return field_def_dict

    def normalize_sf_field_types(self, dataframe, salesforce_model):
        # type: (pd.DataFrame, str) -> pd.DataFrame
        """
        Validate and normalize datatypes and data contents from the Dataframe against
        the expected types / possible values provided by Salesforce in the model definition

        Arguments:
            dataframe (pd.DataFrame): The dataframe of data being normalized
            salesforce_model (str): The name of the salesforce model

        Returns:
            dataframe: The normalized dataframe
        """

        error_messages = []
        error_fatal = False
        field_definitions = self.get_sf_field_definition_dict(salesforce_model)

        def _check_picklist_values(value, picklist):
            """
            Check to make sure a value, or each of the list elements if a list, is in the picklist
            """
            # Check for NaN and handle it before any type conversion
            if pd.isna(value):
                return None
            # Handle float values by converting them to integers
            if isinstance(value, float):
                value = int(value)
            if isinstance(value, list):
                return u",".join([unicode(item) for item in value if (item is not None and item not in picklist)])
            else:
                return value if (value not in [None, ''] and value not in picklist) else None

        # Make sure fields are of appropriate types:
        log.info("Normalizing {} fields for salesforce model {}.".format(len(dataframe.columns), salesforce_model))
        log.debug(dataframe.columns)
        for field_name in dataframe.columns:
            field_definition = field_definitions.get(field_name, None)
            if not field_definition:
                if field_name == "quorum_side_primary_key":
                    # This is where we store the Quorum ID for reference; it isn't actually sent, so skip it
                    continue
                error_messages.append(u"Column {} not valid for salesforce_model {}".format(field_name, salesforce_model))
                # A bad column name will cause ALL rows to fail, so no point allowing the user to force it
                error_fatal = True
                continue

            field_datatype = field_definition["Type"]
            if field_datatype != "bool":
                # Convert all non-boolean falsy values to None
                dataframe[field_name] = dataframe[field_name].map(lambda value: value if not hasattr(value, "__len__") else None if len(value) == 0 else value)
            # logs to the terminal the name of the field in question that is causing the error, if encountered.
            if any(isinstance(x, list) for x in dataframe[field_name].dropna()):
                log.error("Unhashable type detected in field '{}'. Unable to process this field.".format(field_name))
                raise TypeError("Unhashable type detected in field '{}'Apply the List to String preprocessor if this is acceptable.".format(field_name))

            # Now do field-by-field processing on the data
            if field_datatype in ["date", "datetime"]:
                # Check dates/datetimes and then convert values to the specific SF format required
                nondates = list(dataframe[dataframe[field_name].map(
                    lambda value: False if (value and value is not pd.NaT and isinstance(value, (datetime, date))) else True
                )][field_name].unique())
                if nondates:
                    error_messages.append(u"Column {} has Date/Datetime values {} that are not date/datetime values".format(field_name, nondates))
                if field_datatype == "date":
                    # Convert to date specific format
                    dataframe[field_name] = dataframe[field_name].map(
                        lambda value: value.strftime(SF_DATE_FORMAT) if (value and value is not pd.NaT and isinstance(value, (datetime, date))) else None
                    )
                else:
                    # Convert to Datetime specific format
                    dataframe[field_name] = dataframe[field_name].map(
                        lambda value: value.strftime(SF_DATETIME_FORMAT) if (value and value is not pd.NaT and isinstance(value, (datetime, date))) else None
                    )
            elif field_datatype == "picklist":
                # Verify that all of the values are valid picklist entries for this field; some of these could be lists, so check all list elements
                picklist = field_definition.get("Pick List")
                if isinstance(picklist, list):
                    check_this_picklist = partial(_check_picklist_values, picklist=picklist)
                    values_not_in_picklist = list(dataframe[field_name].map(check_this_picklist).unique())
                    if None in values_not_in_picklist:
                        values_not_in_picklist.pop(values_not_in_picklist.index(None))
                    if values_not_in_picklist:
                        error_messages.append(u"Picklist column {} has values {} that are not valid picklist entries.".format(
                            field_name, values_not_in_picklist
                        ))
                else:
                    error_messages.append(u"Field {} is a picklist with no valid values defined".format(field_name))
            elif field_datatype in ['id', u'reference']:
                # Clean out any empty strings from the Id column - SF API requires None, rather than empty string
                wrong_length_rows = list(dataframe[dataframe[field_name].map(
                    lambda x: len(unicode(x)) not in [0, 15, 18] if x else False
                )][field_name].unique())
                if wrong_length_rows:
                    error_messages.append(u"Column {} has ID values {} that are not of an acceptable length".format(field_name, wrong_length_rows))
                    if field_datatype == "id":
                        # Issues with the ID field, used to define which record is being upsert, should be treated as Fatal
                        error_fatal = True
                dataframe[field_name] = dataframe[field_name].map(lambda value: value if value else None)
            elif field_datatype == "boolean":
                non_bool_values = list(dataframe[dataframe[field_name].map(lambda x: x not in [True, False, None, "0", " "])][field_name].unique())
                if non_bool_values:
                    error_messages.append(u"Boolean column {} has values {} that are not valid boolean types.".format(field_name, non_bool_values))
            elif field_datatype in ["int", "float", "currency", "decimal", "double", "percent"]:
                # Consider what to do here
                pass
            elif field_definition["Length"]:
                # At this stage, Assume everything else with a length attribute is a String or variation thereof
                # Check for strings that exceed the maximum length provided for the field, including those that are WAY longer
                not_stringable = list(dataframe[dataframe[field_name].fillna("").map(lambda x: not isinstance(x, (str, unicode, int, float)))][field_name])
                if not_stringable:
                    error_messages.append(
                        u"Column {} has {} values that are not strings, being {}.".format(
                            field_name,
                            len(not_stringable),
                            not_stringable
                        ),
                    )

                def _str_too_long(value, length):
                    try:
                        value = unicode(value)
                        return (len(value) > length)
                    except Exception:
                        return False
                length = field_definition["Length"]
                str_too_long = partial(_str_too_long, length=length)
                strings_over_length = list(dataframe[dataframe[field_name].map(str_too_long)][field_name])
                if strings_over_length:
                    error_messages.append(
                        u"Column {} has {} unique strings over the maximum length of {}, being {}."
                        " Apply the Truncate Column preprocessor if this is acceptable.".format(
                            field_name,
                            len(strings_over_length),
                            length,
                            strings_over_length
                        ),
                    )
            elif field_datatype == "address":
                pass
                # TODO: Figure out what validation is possible here
            elif field_datatype == "base64":
                pass
                # TODO: Figure out what validation is possible here
            else:
                error_messages.append(u"Column {} has an unrecognized data type, {}".format(field_name, field_datatype))

        if error_messages:
            # Communicate to the user what happened, and maybe allow them to proceed with the run if FORCE is set
            log.error(u"There were errors on the run:\n{}".format("\n".join(error_messages)))
            if error_fatal:
                raise ValueError("Data cannot be normalized to SF")

        return dataframe
