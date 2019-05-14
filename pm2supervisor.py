# -*- coding: utf-8 -*-
import json
import logging
import subprocess
import time

logger = logging.getLogger("pm2_to_supervisor")


class SupervisorGroup(object):
    STATUS_STOPPED = 'NOT RUNNING'
    statuses_translator = {
        'online': 'RUNNING',
        'stopping': STATUS_STOPPED,
        'stopped': STATUS_STOPPED,
        'launching': 'STARTING',
    }

    RESTART_CMD = "pm2 restart {}"
    STOP_CMD = "pm2 stop {}"
    REMOVE_CMD = "pm2 delete {}"
    STATUS_CMD = "pm2 show {}"
    ALL_STATUS_CMD = "pm2 jlist"

    DEFAULT_ERROR_MESSAGE = "Error calling `{}`. Command returned code {}"

    def __init__(
        self, group_name, python_path, working_directory, alert_method=None
    ):
        self.children = dict()
        self.group_name = group_name
        self.alert_method = alert_method

        self.START_CMD = (
            "pm2 start " + working_directory +
            "/{} --interpreter " + python_path +
            " --name {} --log-date-format='YYYY-MM-DD::HH:mm:ss:SSS' -- {}"
        )

        self._recover_existent_processes()

    def _recover_existent_processes(self):
        processes = self.get_all_processes()
        group_keyword = "{}:".format(self.group_name)

        for process in processes:
            name = process.get('name', '')
            if name.startswith(group_keyword):
                process['instruction'] = self.RESTART_CMD.format(name)
                self.children[name] = process

    @classmethod
    def _calculate_uptime(cls, since):
        """
        Using a giving timestamp (in millis), calculate the seconds of uptime
        :param since:
        :return:
        """
        now_timestamp = time.time()
        return int(now_timestamp - since/1000)

    @classmethod
    def _parse_pm2_info(cls, process_data):
        """
        Parsing potentially utilizable data from the process
        :param process_data:
        :return:
        """
        process_env = process_data.get('pm2_env', {})
        formatted_data = {
            'name': process_data.get('name'),
            'status': cls.statuses_translator.get(
                process_env.get('status'),
                process_env.get('status')
            ),
            'pm2_status': process_env.get('status'),
            'uptime': cls._calculate_uptime(process_env.get('pm_uptime')),
            'system': {
                'pid': process_data.get('pid'),
                'memory': process_data.get('monit', {}).get('memory')
            },
            'log': {
                'out': process_env.get('pm_out_log_path'),
                'error': process_env.get('pm_err_log_path')
            },
            'execution': {
                'interpreter': process_env.get('exec_interpreter'),
                'command': process_env.get('pm_exec_path'),
                'arguments': process_env.get('args', [])
            }
        }

        return formatted_data

    @classmethod
    def get_all_processes(cls):
        """
        Get all processes on pm2 and return a list of structures containing
        data of each process, like the name, status, and time running.
        :return: list[dict]
        """

        instruction_array = cls.ALL_STATUS_CMD.split(" ")

        result, return_code = cls._run_subprocess(instruction_array)

        processes_list = list()
        if return_code != 0:
            logger.error(
                cls.DEFAULT_ERROR_MESSAGE.format(
                    cls.ALL_STATUS_CMD, str(return_code)
                )
            )
        else:
            try:
                processes = json.loads(result.decode())
            except Exception as e:
                logger.error(
                    "Error parsing stdout. {}".format(e), exc_info=True
                )
            else:
                processes_list = list(map(cls._parse_pm2_info, processes))

        return processes_list

    def list(self):
        """
        Get a dictionary using 'process name' as key and 'process status' as
        value.

        Method exposes as supervisor method.
        :return: dict[str, str]
        """
        processes = self.get_all_processes()

        processes_summary = dict()
        for process in processes:
            processes_summary[process.get('name')] = process.get('status')

        group_processes = dict()

        for key, value in self.children.items():
            status = processes_summary.get(key, self.STATUS_STOPPED)
            name_process = key.split(":")[-1]
            group_processes[name_process] = status

        return group_processes

    def status(self, process_name, force_update=False):
        """
        Get a list with the process status.
        Method exposes as supervisor method.
        :param process_name:
        :return: list[str]
        """
        if force_update:
            self._recover_existent_processes()
        process_fullname = '{}:{}'.format(self.group_name, process_name)
        process = self.children.get(process_fullname, None)

        if process is None:
            return [self.STATUS_STOPPED]

        return [process.get('status', self.STATUS_STOPPED)]

    def stop(self, process_name):
        """
        Stop a child process.
        Method exposes as supervisor method
        :param process_name:
        :return:
        """

        process_fullname = '{}:{}'.format(self.group_name, process_name)
        instruction = self.STOP_CMD.format(process_fullname)
        instruction_array = instruction.split(" ")

        _, return_code = self._run_subprocess(instruction_array)

        if return_code != 0:
            logger.error(
                self.DEFAULT_ERROR_MESSAGE.format(
                    instruction, str(return_code)
                )
            )
            return False
        else:
            data = self.children.get(process_fullname, None)

            if data is None:
                self.alert_mail('The stopped process {} was not a child'.format(process_fullname))
                return False
            else:
                data['status'] = 'NOT RUNNING'

        return True

    def remove(self, process_name):
        """
        Remove a child process, killing it.
        Method exposes as supervisor method
        :param process_name:
        :return:
        """

        process_fullname = '{}:{}'.format(self.group_name, process_name)
        instruction = self.REMOVE_CMD.format(process_fullname)
        instruction_array = instruction.split(" ")

        _, return_code = self._run_subprocess(instruction_array)

        if return_code != 0:
            logger.error(
                self.DEFAULT_ERROR_MESSAGE.format(
                    instruction, str(return_code)
                )
            )
            return False
        else:
            if process_fullname in self.children:
                del self.children[process_fullname]

        return True

    def start(self, process_name):
        """
        Start a child process name
        Method exposes as supervisor method
        :param process_name:
        :return:
        """

        process_fullname = '{}:{}'.format(self.group_name, process_name)
        logger.debug('Starting {}'.format(process_fullname))

        data = self.children.get(process_fullname, None)

        if data is None:
            logger.error('Process doesnt exist: {}'.format(process_fullname))
            return False

        data['status'] = 'STARTING'
        instruction = data.get('instruction', '')
        instruction_array = instruction.split(" ")

        _, return_code = self._run_subprocess(instruction_array)

        if return_code != 0:
            logger.error(
                self.DEFAULT_ERROR_MESSAGE.format(
                    instruction, str(return_code)
                )
            )
            return False
        else:
            data['status'] = 'RUNNING'

        return True

    def create(self, process_name, commands):
        """
        Add a child to be executed later.
        Method exposes as supervisor method
        :param process_name:
        :param commands:
        :return:
        """
        process_fullname = '{}:{}'.format(self.group_name, process_name)
        logger.debug(
            "Adding process {} with command {}".format(
                process_fullname, " ".join(commands)
            )
        )

        program_to_exec = commands[0]
        program_args = " ".join(commands[1:])
        instruction = self.START_CMD.format(
            program_to_exec, process_fullname, program_args
        )

        process = self.children.get(process_fullname, None)

        if process is None:
            self.children[process_fullname] = {
                'name': process_fullname,
                'instruction': instruction,
                'status': 'NOT RUNNING'
            }
        else:
            logger.debug('Process already exists')

        return self.start(process_name)

    def alert_mail(self, message):
        """
        Send an alert in the configured way. Usually a though a logger.
        Method exposes as supervisor method
        :param message:
        :return:
        """
        alert = "[GROUP {}] {}".format(self.group_name, message)
        logger.error(alert)
        if self.alert_method is not None:
            try:
                self.alert_method(alert)
            except Exception as e:
                logger.error(
                    "Exception in external alert method. {}".format(e),
                    exc_info=True
                )

    def create_new_process(self, process):
        """
        Method to create a process using a SupervisorSubProcess object.
        Method exposes as supervisor method
        :param process: SupervisorSubProcess
        :return:
        """
        if not isinstance(process, SupervisorSubProcess):
            logger.error("Wrong instance of SupervisorSubProcess")
            return False
        return self.create(process.name, process.commands)

    @classmethod
    def get_pm2_status(cls, process_name):
        """
        Return the pm2 status of the process
        :param process_name:
        :return:
        """
        process = cls.get_process_information(process_name)
        if process is not None:
            return process.get('pm2_status', None)
        return None

    @classmethod
    def get_process_information(cls, process_name):
        """
        Return the filtered information of a pm2 process
        including only the essential
        :param process_name:
        :return:
        """
        processes = cls.get_all_processes()
        for process in processes:
            if process.get('name') == process_name:
                return process
        return None

    @classmethod
    def _run_subprocess(cls, instructions):
        if hasattr(subprocess, "run"):
            execution = subprocess.run(instructions, stdout=subprocess.PIPE)
            result = execution.stdout
            return_code = execution.returncode
        else:
            try:
                result = subprocess.check_output(instructions)
                return_code = 0
            except subprocess.CalledProcessError as error:
                result = error.output
                return_code = error.returncode

        return result, return_code


class SupervisorSubProcess(object):

    def __init__(self, process_name, command=None):
        """
        Create a object to host the process.
        :param process_name:
        :param command: usually the file to exec and the arguments.
        """
        list_of_commands = None
        if command is not None:
            list_of_commands = command.split(" ")

        self.name = process_name
        self.command = command
        self.commands = list_of_commands
