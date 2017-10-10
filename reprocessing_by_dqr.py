"""
This script assists in reprocessing tasks
Author: asr
"""

# Import required libraries
import logging
import os
import sys
import datetime

# try importing armlib
try:
    import armlib
except ImportError:
    armlib = os.path.join('/apps', 'adc', 'lib')
    if not os.path.isdir(armlib):
        assert False, 'cannot find armlib'
    sys.path.insert(0, armlib)

from armlib.config import load_config
from armlib.database import ARMNamedTupleAutocommitConnection


class ReprocessingDQR(object):
    def __init__(self, postgres_config):
        # for logging and db connection
        self.logger = logging.getLogger('root.reprocessing')
        self.conn = ARMNamedTupleAutocommitConnection(**postgres_config)

        self.dqr_id = 'D170712.3'

        # start and end dates of DQR
        self.start_date = ""
        self.end_date = ""

        # affected datastreams
        self.affected_ds = []

        # affected verioned filenames
        self.affected_files = []

        # email lists
        self.cc_list = ['Laura.Riihimaki@pnnl.gov',
                        'Chitra.Sivaraman@pnnl.gov',
                        'Yan.Shi@pnnl.gov',
                        'mjensen@bnl.gov',
                        'dlsisterson@anl.gov',
                        'jim.mather@pnl.gov',
                        'jwmonroe@ou.edu',
                        'kkehoe@gcn.ou.edu',
                        'rpeppler@ou.edu',
                        'atheisen@ou.edu',
                        'jacksonwl@ornl.gov',
                        'nicole.keck@pnnl.gov',
                        'Annette.Koontz@pnnl.gov',
                        'jackie.marshall@pnnl.gov',
                        'tonya.martin@pnnl.gov',
                        'ken.burk@pnnl.gov',
                        'joshua.howie@pnnl.gov',
                        'palanisamyg@ornl.gov']

        self.to_users = []
        self.bcc_users = []

    def find_time_period(self):
        """
        This method finds the start and end date and time of a DQR
        """
        args = [self.dqr_id]
        sql_time = "SELECT  distinct start_date, end_date " \
                   "FROM pifcardqr2.varname_metric " \
                   "WHERE id = %s"

        results_tp = self.conn.fexecute(sql_time, args)

        for entry in results_tp:
            self.start_date = datetime.datetime.strftime(entry.start_date, '%Y-%m-%d')
            self.end_date = datetime.datetime.strftime(entry.end_date, '%Y-%m-%d')
            print(self.start_date)
            print(self.end_date)

    def find_affected_ds(self):

        args = [self.dqr_id]
        sql_ds = "SELECT distinct datastream " \
                 "FROM pifcardqr2.varname_metric " \
                 "WHERE id = %s"

        results_ds = self.conn.fexecute(sql_ds, args)
        # print(results_ds)

        for entry in results_ds:
            self.affected_ds.append(entry)
        # print(self.affected_ds)

    def find_affected_files(self):

        for entry in self.affected_ds:
            file_name = str(entry[0]) + '%'
            # print(file_name)

            args = [file_name, self.start_date, self.end_date]
            sql_fn = "SELECT versioned_filename " \
                     "FROM data_reception.file_contents " \
                     "WHERE versioned_filename LIKE %s and " \
                     "start_time >= %s and end_time <= %s"
            results_fn = self.conn.fexecute(sql_fn, args)
            for row in results_fn:
                self.affected_files.append(row)
        # print(self.affected_files)

    def find_users_bcc(self):

        for file in self.affected_files:
            args = [file, self.start_date]
            """
            # this is pointed towards user_history
            sql_sessid = "SELECT distinct user_email " \
                         "FROM arm.current_retrievals cr " \
                         "inner join arm.retrieval_archive ra on ra.session_id=cr.session_id " \
                         "inner join arm.user_history p on ra.arch_user_id = p.arch_user_id " \
                         "WHERE new_filename = %s and session_date >= %s"
            """
            # this is pointed to people.people
            sql_sessid = "SELECT distinct email " \
                         "FROM arm.current_retrievals cr " \
                         "inner join arm.retrieval_archive ra on ra.session_id=cr.session_id " \
                         "inner join people.people p on ra.arch_user_id = p.arch_user_id " \
                         "WHERE new_filename = %s and session_date >= %s"

            results_sess_id = self.conn.fexecute(sql_sessid, args)

            # Adding users in bcc list
            for entry in results_sess_id:
                # print(entry.user_email)
                self.bcc_users.append(entry.user_email)
        print("BCC Users:  " + str(self.bcc_users))

    def find_users_to(self):
        for ds in self.affected_ds:
            args = [ds]
            sql_to = "select distinct email " \
                     "from people.people p " \
                     "inner join people.group_role gr on gr.person_id = p.person_id " \
                     "inner join arm_int2.datastream_info di on upper(di.instrument_code) = gr.role_name " \
                     "where datastream = %s"

            results_to = self.conn.fexecute(sql_to, args)

            for entry in results_to:
                self.to_users.append(entry.email)
        print("To Users:  " + str(self.to_users))

    def reprocess(self):
        self.find_time_period()
        self.find_affected_ds()
        self.find_affected_files()
        self.find_users_bcc()
        self.find_users_to()
        pass


def do_setup():
    """
    This method does the initial setup for authentication etc
    """
    file_path = os.path.dirname(os.path.abspath(__file__))
    config_file_path = 'configFiles/metrics.ini'
    config_path = os.path.join(file_path, config_file_path)
    config = load_config(config_path)
    postgres_config = config['postgres']
    postgres_config['autocommit'] = True
    return postgres_config


def main():
    postgres_config = do_setup()
    reprocessing_dqr = ReprocessingDQR(postgres_config)
    reprocessing_dqr.reprocess()

# Calling main method
main()