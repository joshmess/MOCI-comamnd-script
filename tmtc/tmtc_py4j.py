"""Access the GenerationOne TM/TC API via a Py4J connection to a JVM

Interact with the onboard software via the GenerationOne component
interface:
  - Get and Set the values of onboard parameters
  - Query the dimensions of an onboard parameter
  - Invoke onboard actions
  - Uplink and Downlink large parameters over multiple transfer frames
  - Register to receive onboard Event, Housekeeping and Debug telemetry

Explore the spacecraft database:
  - Lookup onboard identifiers
  - Fetch descriptions of onboard components, events, actions parameters, etc.

"""

__author__ = 'Alex Mason'
__copyright__ = 'Copyright (c) Bright Ascension Ltd, 2018'

import functools
import logging
import os
import queue
import struct
import tempfile
from builtins import property
from collections import deque, Iterable
from enum import Enum
from typing import Union, NamedTuple, List, Callable

from py4j.java_collections import ListConverter
from py4j.java_gateway import JavaGateway, JavaObject, CallbackServerParameters
from py4j.protocol import Py4JError, Py4JJavaError


# Exceptions used by this module.
class TMTCServerError(OSError):
    """Base class used for exceptions raised by this module related to
       communication with the TMTC Server

    """


class TMTCDisconnected(TMTCServerError):
    """Not Connected to TMTC server

    This exception is raised when the server unexpectedly disconnects,
    or when an attempt is made to use the TMTC instance before
    connecting it to a server.

    """


class TMTCModelQueryError(TMTCServerError):
    """Raised when an error occurs while attempting to query the spacecraft
    database/deployment model

    """


class TMTCCommandError(TMTCServerError):
    """Raised when an error occurs while performing a TMTC command

    This is typically because the connection between the TMTC Server and the
    on-board software deployment has been broken

    """


class TMTCCommandTimeout(TMTCCommandError):
    """Raised when no response was received for a TMTC command within the
    specified timeout period

    """


class TMTCTransferError(TMTCServerError):
    """Raised when an error occurs while a transfer is in progress"""


class OBSWExceptionError(RuntimeError):
    """Raised when an exception code is returned by an on-board function"""


class Py4JConnection:
    """Wrapper class for handling the Py4J gateway to the TMTCLib Java API

    The Py4J connection is used to access the functions exposed by the TMTCLib
    Java API.

    Handles setting up and shutting down the Java Virtual Machine, and exposes
    convenient jvm views as properties for relevant java packages.

    As starting a JVM takes a relatively long time we memoize the launch of
    the JVM.

    """

    def __init__(self, classpath, javaopts=None):
        """
        :param classpath: Classpath used to launch the Java Gateway
                          This should be the path to the TMTCLib jar file
                          (GNDSW/TMTCLib/target/TMTCLib-18.4.jar)
        :param javaopts: Optionally specify an array of options to pass to Java

        """

        self._classpath = classpath
        self._javaopts = javaopts

        self._gw, self._jvm_stderr, self._jvm_stdout = self.launch(
            classpath,
            javaopts)

    @property
    def jvm(self):
        return self._gw.jvm

    @property
    def j_protocol(self):
        return self._gw.jvm.com.brightascension.gen1.protocol

    @property
    def j_model(self):
        return self._gw.jvm.com.brightascension.gen1.model

    @property
    def j_util(self):
        return self._gw.jvm.com.brightascension.gen1.util

    @property
    def gateway_client(self):
        # noinspection PyProtectedMember
        return self._gw._gateway_client

    def to_java_list(self, python_list):
        """ Convert a python list to a Java collection object"""

        # noinspection PyProtectedMember
        return ListConverter().convert(python_list, self._gw._gateway_client)

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def launch(classpath, javaopts):
        """Start a JVM process that will be killed when python exits

        The launch of a JVM is managed by a TMTCPy4J object, and a single JVM
        instance is shared by all TMTCPy4J instances. Once all python threads
        have stopped the JVM is automatically shut down.
        """

        logging.info('Launching JVM...')

        if javaopts == 'debug':
            # noinspection SpellCheckingInspection
            javaopts = [
                '-Xdebug',
                '-Xrunjdwp:transport=dt_socket,quiet=y,'
                'server=y,suspend=n,address=9009']

        elif javaopts is None:
            javaopts = []

        # Circular buffers to capture most recent java side output for debug
        jvm_stderr = deque(maxlen=512)
        jvm_stdout = deque(maxlen=512)

        gw = JavaGateway.launch_gateway(
            classpath=classpath,
            die_on_exit=True,
            redirect_stdout=jvm_stdout,
            redirect_stderr=jvm_stderr,
            daemonize_redirect=True,
            javaopts=javaopts)

        callback_server_parameters = CallbackServerParameters(
            propagate_java_exceptions=True,
            daemonize=True,
            daemonize_connections=True)

        gw.start_callback_server(callback_server_parameters)

        logging.info('JVM started')

        return gw, jvm_stdout, jvm_stderr

    def drain_jvm_stderr(self):
        """Debug function, prints most recent JVM stderr"""
        try:
            for line in iter(self._jvm_stderr.pop, None):
                print(line, end='')  # newline already in str
        except IndexError:
            # Swallow error
            pass

    def drain_jvm_stdout(self):
        """Debug function, prints most recent JVM stdout"""
        try:
            for line in iter(self._jvm_stdout.pop, None):
                print(line, end='')  # newline already in str
        except IndexError:
            # Swallow error
            pass


##
# Model inspection types used in this module
# When we move to support python >= 3.7 these should be upgraded
# to dataclasses
##

Documentation = NamedTuple('Documentation', [('text', str), ('html', str)])
Documentation.text.__doc__ = 'Documentation as plain text'
Documentation.html.__doc__ = 'Documentation as html'

# noinspection SpellCheckingInspection
_type_strings = [
    'unsigned',
    'signed',
    'float',
    'parameterref',
    'bitfield',
    'raw',
    'varaw']
# noinspection PyArgumentList
TypeStr = Enum('TypeStr', _type_strings, module=__name__)

_type_classes = ['value', 'fixed_raw', 'var_raw', 'fixed_vector', 'var_vector']
# noinspection PyArgumentList
TypeClass = Enum('TypeClass', _type_classes, module=__name__)

_argument_type_classes = ['fixed', 'variable']
# noinspection PyArgumentList
ArgumentTypeClass = Enum(
    'ArgumentTypeClass',
    _argument_type_classes,
    module=__name__)

ParameterSize = NamedTuple('ParameterSize', [('size', int), ('length', int)])
ParameterSize.__doc__ = 'Describes the dimensions of a parameter'
ParameterSize.size.__doc__ = 'the overall size (in bytes) of the parameter'
ParameterSize.length.__doc__ = 'the length (number of rows) of the parameter'

_element_attrs = [
    ('id', int),
    ('name', str),
    ('full_name', str),
    ('description', str),
    ('documentation', Documentation),
    ('index', int)
]  # Common attributes for elements present in the model

_argument_attrs = [
    ('signature', str),
    ('min_bytes', int),
    ('max_bytes', int),
    ('type_class', ArgumentTypeClass),
    ('is_fixed_size', bool)]
Argument = NamedTuple(
    'Argument', _element_attrs + _argument_attrs)
Argument.__doc__ += ': Description of an argument to an action'

_action_definition_attrs = [
    ('signature', str),
    ('arguments', List[Argument])]
ActionDefinition = NamedTuple('ActionDefinition', _action_definition_attrs)
ActionDefinition.__doc__ += ": Description of an action's type"
ActionInstance = NamedTuple(
    'ActionInstance', _element_attrs + [('definition', ActionDefinition)])
ActionInstance.__doc__ += ''
': Description of an instance of an action within a deployment'

_parameter_definition_attrs = [
    ('signature', str),
    ('type_str', TypeStr),
    ('type_class', TypeClass),
    ('min_rows', int),
    ('max_rows', int),
    ('bits_per_row', int),
    ('bytes_per_row', int),
    ('storage_bytes_per_row', int),
    ('unused_bits_per_row', int),
    ('is_raw', bool),
    ('is_fixed_size', bool),
    ('is_read_only', bool),
    ('is_config', bool),
]
ParameterDefinition = NamedTuple(
    'ParameterDefinition',
    _parameter_definition_attrs)
ParameterDefinition.__doc__ += ""
": Description of a parameter in terms of it's type"
ParameterInstance = NamedTuple(
    'ParameterInstance', _element_attrs + [('definition', ParameterDefinition)])
ParameterInstance.__doc__ += ''
': Description of an instance of a parameter within a deployment'

ParameterBlockInstance = NamedTuple(
    'ParameterBlockInstance',
    _element_attrs + [('definition', ParameterDefinition)])
ParameterBlockInstance.__doc__ += ''
': Description of an instance of a parameter block within a deployment'

_event_definition_attrs = [
    ('signature', str),
    ('severity', str),
]
EventDefinition = NamedTuple(
    'EventDefinition',
    _event_definition_attrs)
EventDefinition.__doc__ += ": Description of an event"
EventInstance = NamedTuple(
    'EventInstance',
    _element_attrs + [('definition', EventDefinition)])
EventInstance.__doc__ += ''
': Description of an instance of an event within a deployment'

_event_source_definition_attrs = [
    ('signature', str),
]
EventSourceDefinition = NamedTuple('EventSourceDefinition',
                                   _event_source_definition_attrs)
EventSourceDefinition.__doc__ += ": Description of an event source"
EventSourceInstance = NamedTuple(
    'EventSourceInstance',
    _element_attrs + [('definition', EventSourceDefinition)])
EventSourceInstance.__doc__ += ''
': Description of an instance of an event source within a deployment'

OnboardException = NamedTuple('OnboardException', _element_attrs)

_component_attrs = [
    ('signature', str),
    ('actions', List[ActionInstance]),
    ('parameters', List[ParameterInstance]),
    ('events', List[EventInstance]),
    ('event_sources', List[EventSourceInstance]),
    ('exceptions', List[OnboardException])
]
ComponentInstance = NamedTuple(
    'ComponentInstance', _element_attrs + _component_attrs)

_component_group_attrs = [
    ('signature', str),
    ('components', List[ComponentInstance]),
    ('component_groups', List['ComponentGroup'])]
ComponentGroup = NamedTuple(
    'ComponentGroup', _element_attrs + _component_group_attrs)

_deployment_attrs = [
    ('name', str),
    ('description', str),
    ('documentation', Documentation),
    ('components', List[ComponentInstance]),
    ('component_groups', List['ComponentGroup'])]
DeploymentInstance = NamedTuple(
    'DeploymentInstance', _deployment_attrs)


def _element_fac(python_cls, java_element: JavaObject, id_: int, **kwargs):
    """ Create python type from java element """

    return python_cls(
        id=id_,
        name=java_element.getName(),
        full_name=java_element.getFullName(),
        description=java_element.getDescription(),
        documentation=Documentation(
            text=java_element.getDocumentationAsText(),
            html=java_element.getDocumentationAsHtml()),
        index=java_element.getIndex(),
        **kwargs)


def _lookup_tree_get_id(d):
    """Used once we have an unambiguous name sequence
       Recurse down dictionaries until we get to the parameter ID
       """
    # todo: this is convoluted- consider reimplementing with a trie
    try:
        next_dict = d[list(d.keys())[0]]
        return _lookup_tree_get_id(next_dict)
    except (TypeError, AttributeError):
        # Not a dictionary, we have reached the parameter ID
        param_id = d
        return param_id


def _lookup_tree_get(tree, keys):
    try:
        return functools.reduce(dict.get, keys.split('.')[::-1], tree)
    except TypeError:
        return None


def _lookup_tree_put(tree, keys, value):
    if '.' in keys:
        key, rest = keys.split(".", 1)

        if key not in tree:
            tree[key] = {}

        _lookup_tree_put(tree[key], rest, value)

    else:
        tree[keys] = value


def _requires_scdb(method):
    """Decorator: ensure we don't attempt to access SCDB before we load it"""

    @functools.wraps(method)
    def wrapper(self, *method_args, **method_kwargs):
        if hasattr(self, 'j_deployment_instance') and \
                self.j_deployment_instance:
            return method(self, *method_args, **method_kwargs)
        raise TMTCModelQueryError("Spacecraft Database file not loaded")

    return wrapper


class ModelInspector:
    """Class used to query the details of a deployment's spacecraft database

    It is not expected that this class will be used directly by the user.
    Instead, create a TMTCPy4j instance and use its 'model' attribute.

    """

    def __init__(self, gateway: Py4JConnection, scdb_filename: str):
        self._gw = gateway

        # Java objects
        self.j_model = None
        self.j_deployment_instance = None

        # Lookup trees for partial name matches (implemented on python side)
        self.action_name_lookup_tree = None
        self.event_name_lookup_tree = None
        self.event_source_name_lookup_tree = None
        self.onboard_exception_name_lookup_tree = None
        self.component_name_lookup_tree = None
        self.component_group_name_lookup_tree = None
        self.parameter_name_lookup_tree = None
        self.parameter_block_name_lookup_tree = None

        self.load_model_from_scdb(scdb_filename)

    @_requires_scdb
    def name_to_action_id(self, action_name: str) -> int:
        """Lookup an action's onboard id from a name

        :param action_name: action name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(action_name,
                                self.j_deployment_instance.getActions(),
                                self.action_name_lookup_tree)

    @_requires_scdb
    def name_to_component_id(self, component_name: str) -> int:
        """Lookup a component instances's onboard id from a name

        Useful when setting up onboard configurations

        :param component_name: component instance name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(component_name,
                                self.j_deployment_instance.getComponents(),
                                self.component_name_lookup_tree)

    @_requires_scdb
    def name_to_component_group_id(self, component_group_name: str) -> int:
        """Lookup a component group's id from a name

        (Included for ground side scripting. Not used when interacting with
        onboard software)

        :param component_group_name: component group name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: identifier

        """
        return self._name_to_id(component_group_name,
                                self.j_deployment_instance.getComponentGroups(),
                                self.component_group_name_lookup_tree)

    @_requires_scdb
    def name_to_event_id(self, event_name: str) -> int:
        """Lookup an event's onboard id from a name

        :param event_name: event name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(event_name,
                                self.j_deployment_instance.getEvents(),
                                self.event_name_lookup_tree)

    @_requires_scdb
    def name_to_event_source_id(self, event_source_name: str) -> int:
        """Lookup an event source's onboard id from a name

        :param event_source_name: event source name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(event_source_name,
                                self.j_deployment_instance.getEventSources(),
                                self.event_source_name_lookup_tree)

    @_requires_scdb
    def name_to_onboard_exception_id(self, onboard_exception: str) -> int:
        """Lookup an event source's onboard id from a name

        :param onboard_exception: exception name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(onboard_exception,
                                self.j_deployment_instance.getExceptions(),
                                self.onboard_exception_name_lookup_tree)

    @_requires_scdb
    def name_to_parameter_id(self, parameter_name: str) -> int:
        """Lookup a parameter's onboard id from a name

        :param parameter_name: parameter name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(parameter_name,
                                self.j_deployment_instance.getParameters(),
                                self.parameter_name_lookup_tree)

    @_requires_scdb
    def name_to_parameter_block_id(self, param_block_name: str) -> int:
        """Lookup a parameter block's onboard id from a name

        :param param_block_name: parameter block name

        :raises TMTCModelQueryError if name cannot be matched to identifier

        :return: onboard identifier

        """
        return self._name_to_id(param_block_name,
                                self.j_deployment_instance.getParameterBlocks(),
                                self.parameter_block_name_lookup_tree)

    @_requires_scdb
    def id_to_action_name(self, action_id: int) -> str:
        """ Lookup an action's name using it's onboard identifier

        :param action_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: action name

        """
        return self._id_to_name(
            action_id,
            self.j_deployment_instance.getActions())

    @_requires_scdb
    def id_to_component_name(self, component_id: int) -> str:
        """ Lookup a component's name using it's onboard identifier

        :param component_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: component name

        """
        return self._id_to_name(
            component_id,
            self.j_deployment_instance.getComponents())

    @_requires_scdb
    def id_to_component_group_name(self, component_group_id: int) -> str:
        """ Lookup a component groups's name using it's identifier

        (Included for ground side scripting. Not used when interacting with
        onboard software)

        :param component_group_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: component group name

        """
        return self._id_to_name(
            component_group_id,
            self.j_deployment_instance.getComponentGroups())

    @_requires_scdb
    def id_to_event_name(self, event_id: int) -> str:
        """ Lookup an event's name using it's onboard identifier

        :param event_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: event name

        """
        return self._id_to_name(
            event_id,
            self.j_deployment_instance.getEvents())

    @_requires_scdb
    def id_to_event_source_name(self, event_source_id: int) -> str:
        """ Lookup an event source's name using it's onboard identifier

        :param event_source_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: event source name

        """
        return self._id_to_name(
            event_source_id,
            self.j_deployment_instance.getEventSources())

    @_requires_scdb
    def id_to_onboard_exception_name(self, exception_id: int) -> str:
        """ Lookup an onboard exception's name using it's onboard identifier

        :param exception_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: exception name

        """
        return self._id_to_name(
            exception_id,
            self.j_deployment_instance.getExceptions())

    @_requires_scdb
    def id_to_parameter_name(self, parameter_id: int) -> str:
        """ Lookup a parameter's name using it's onboard identifier

        :param parameter_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: parameter name

        """
        return self._id_to_name(
            parameter_id,
            self.j_deployment_instance.getParameters())

    @_requires_scdb
    def id_to_parameter_block_name(self, parameter_block_id: int) -> str:
        """ Lookup a parameter block's name using it's onboard identifier

        :param parameter_block_id: onboard identifier

        :raises TMTCModelQueryError if identifier not found in database

        :return: parameter block name

        """
        return self._id_to_name(
            parameter_block_id,
            self.j_deployment_instance.getParameterBlocks())

    @_requires_scdb
    def argument(
            self,
            action: Union[int, str],
            argument: [int, str] = 0) -> Union[Argument, None]:
        """Lookup an argument to an action and return details about its type

        :param action: action name or identifier to lookup argument for
        :param argument: argument name or index (defaults to 0).
            Currently actions only have a single argument with index 0
        :return: argument description or None if action takes no arguments

        """
        if isinstance(action, str):
            action = self.name_to_action_id(action)
        if type(action) is not int:
            raise TypeError(
                'argument() requires action to be str or int')

        if isinstance(argument, str):
            j_arguments = self.j_deployment_instance.getActions().getById(
                action).getType().getArguments()

            argument_name_lookup_tree = self._init_name_lookup_tree(j_arguments)

            argument = self._name_to_id(
                argument, j_arguments, argument_name_lookup_tree)

        if type(argument) is not int:
            raise TypeError(
                'argument() requires argument_id to be str or int')

        arg = self.j_deployment_instance.getActions().getById(
            action).getType().getArguments().getById(argument)

        try:
            if arg.getArgumentClass() == \
                    self._gw.j_model.type.ArgumentTypeClass.VARIABLE:
                type_class = ArgumentTypeClass.variable
            else:
                type_class = ArgumentTypeClass.fixed

            return _element_fac(
                Argument,
                arg,
                argument,
                **{'signature': arg.getSignature(),
                   'max_bytes': arg.getMaxBytes(),
                   'min_bytes': arg.getMinBytes(),
                   'is_fixed_size': arg.isFixedSize(),
                   'type_class': type_class})

        except AttributeError:
            # Argument was None
            return None

    @_requires_scdb
    def action_instance(self, action: Union[int, str]) -> ActionInstance:
        """Lookup an action instance and return details about its type

        :param action: name or identifier of action instance to lookup

        :return: action description

        """
        if isinstance(action, str):
            action = self.name_to_action_id(action)
        if type(action) is not int:
            raise TypeError(
                'action_instance(action) requires action to be str or int')

        j_action_inst = self.j_deployment_instance.getActions().getById(action)
        if j_action_inst is None:
            raise TMTCModelQueryError(
                'Action {} not present in deployment'.format(action))

        j_action_type = j_action_inst.getType()

        args = []
        j_args_iterator = j_action_type.getArguments().getAllById().iterator()
        while j_args_iterator.hasNext():
            j_arg = j_args_iterator.next()
            args.append(self.argument(action, j_arg.getId()))

        action_def = ActionDefinition(
            signature=j_action_type.getSignature(),
            arguments=args)

        return _element_fac(ActionInstance,
                            j_action_inst,
                            action,
                            **{'definition': action_def})

    @_requires_scdb
    def component_instance(
            self,
            component_instance: Union[int, str]) -> ComponentInstance:
        """Lookup a component instance and return details about it

        :param component_instance: name or identifier of component instance to
            lookup

        :return: component instance description

        """
        if isinstance(component_instance, str):
            component_instance = self.name_to_component_id(component_instance)
        if type(component_instance) is not int:
            raise TypeError(
                'component_instance(component_instance) '
                'requires component_instance to be str or int')

        j_component_inst = \
            self.j_deployment_instance.getComponents().getById(
                component_instance)
        if j_component_inst is None:
            raise TMTCModelQueryError(
                'Component Instance {} not present in deployment'.format(
                    component_instance))

        actions = [
            self.action_instance(j_action.getId()) for
            j_action in
            j_component_inst.getActions().getAllById().iterator()]

        parameters = [
            self.parameter_instance(j_param.getId()) for
            j_param in
            j_component_inst.getParameters().getAllById().iterator()]

        events = [
            self.event_instance(j_event.getId()) for
            j_event in
            j_component_inst.getEvents().getAllById().iterator()]

        event_sources = [
            self.event_source_instance(j_event_source.getId()) for
            j_event_source in
            j_component_inst.getEventSources().getAllById().iterator()]

        exceptions = [
            self.onboard_exception(j_exception.getId()) for
            j_exception in
            j_component_inst.getExceptions().getAllById().iterator()]

        return _element_fac(
            ComponentInstance,
            j_component_inst,
            component_instance,
            **{'signature': j_component_inst.getType().getSignature(),
               'actions': actions,
               'parameters': parameters,
               'events': events,
               'event_sources': event_sources,
               'exceptions': exceptions,
               })

    @_requires_scdb
    def component_group(
            self,
            component_group: Union[int, str]) -> ComponentGroup:
        """Lookup a component group and return details about it

        (Included for ground side scripting. Not used when interacting with
        onboard software)

        :param component_group: name or identifier of component group to
            lookup

        :return: component group details. Includes lists of component instances
            and component sub groups.

        """
        if isinstance(component_group, str):
            component_group = self.name_to_component_group_id(component_group)
        if type(component_group) is not int:
            raise TypeError(
                'component_group(component_group) '
                'requires component_group to be str or int')

        j_component_group_inst = \
            self.j_deployment_instance.getComponentGroups().getById(
                component_group)
        if j_component_group_inst is None:
            raise TMTCModelQueryError(
                'Component Group {} not present in deployment'.format(
                    component_group))

        def _get_group_for_component_group_inst_id(
                j_groups_: JavaObject,
                target_group_id: int):

            j_groups_lst = [x for x in j_groups_.iterator()]
            for j_group_ in j_groups_lst:
                if j_group_.getComponentGroup().getId() == target_group_id:
                    return j_group_
                else:
                    # Not at this level- recurse
                    j_group_ = _get_group_for_component_group_inst_id(
                        j_group_.getGroups(),
                        target_group_id)

                    if j_group_ is not None:
                        # Keep searching at this level unless we found match
                        return j_group_

        # Walk deployment hierarchy until we reach our group
        j_top_level_groups = \
            self.j_deployment_instance.getComponentTree().getGroups()

        j_group = _get_group_for_component_group_inst_id(
            j_top_level_groups,
            component_group)

        # Reached our group:  fill out component instances and recurse into
        # tree to fill out sub-groups. This is particularly inefficient, but
        # will do until we support the COAST model.

        # get component instances
        components = [
            self.component_instance(j_component_inst.getId()) for
            j_component_inst in
            j_group.getComponents().iterator()]

        # get component groups
        component_groups = [
            self.component_group(j_group.getComponentGroup().getId()) for
            j_group in
            j_group.getGroups().iterator()
        ]

        return _element_fac(
            ComponentGroup,
            j_component_group_inst,
            component_group,
            **{'signature': j_component_group_inst.getType().getSignature(),
               'components': components,
               'component_groups': component_groups,
               })

    @_requires_scdb
    def deployment_instance(self) -> DeploymentInstance:

        j_depl = self.j_deployment_instance

        # get component instances
        components = [
            self.component_instance(j_component_inst.getId()) for
            j_component_inst in
            j_depl.getComponentTree().getComponents().iterator()]

        # get component groups
        component_groups = [
            self.component_group(j_group.getComponentGroup().getId()) for
            j_group in
            j_depl.getComponentTree().getGroups().iterator()
        ]

        return DeploymentInstance(
            name=j_depl.getName(),
            description=j_depl.getDescription(),
            documentation=Documentation(
                text=j_depl.getDocumentationAsText(),
                html=j_depl.getDocumentationAsHtml()),
            components=components,
            component_groups=component_groups
        )

    @_requires_scdb
    def event_instance(self, event: Union[int, str]) -> EventInstance:
        """Lookup an event instance and return details about its type

        :param event: name or identifier of event instance to lookup

        :return: event description

        """
        if isinstance(event, str):
            event = self.name_to_event_id(event)
        if type(event) is not int:
            raise TypeError(
                'event_instance(event) requires event to be str or int')

        j_event_inst = self.j_deployment_instance.getEvents().getById(event)
        if j_event_inst is None:
            raise TMTCModelQueryError(
                'Event {} not present in deployment'.format(event))

        j_event_type = j_event_inst.getType()

        severity_mask = j_event_type.getSeverityMask()
        severity_masks = {0x0000: 'info',
                          0x4000: 'error',
                          0x8000: 'component_fatal',
                          0xC000: 'system_fatal'}

        event_def = EventDefinition(
            signature=j_event_type.getSignature(),
            severity=severity_masks[severity_mask])

        return _element_fac(EventInstance,
                            j_event_inst,
                            event,
                            **{'definition': event_def})

    @_requires_scdb
    def event_source_instance(
            self,
            event_source: Union[int, str]) -> EventSourceInstance:
        """Lookup an event source instance and return details about it

        :param event_source: name or identifier of event source instance to
            lookup

        :return: event source description

        """
        if isinstance(event_source, str):
            event_source = self.name_to_event_source_id(event_source)
        if type(event_source) is not int:
            raise TypeError(
                'event_source_instance(event_source) '
                'requires event to be str or int')

        j_event_source_inst = \
            self.j_deployment_instance.getEventSources().getById(event_source)
        if j_event_source_inst is None:
            raise TMTCModelQueryError(
                'Event Source {} not present in deployment'.format(
                    event_source))

        j_event_source_type = j_event_source_inst.getType()

        event_source_def = EventSourceDefinition(
            signature=j_event_source_type.getSignature())

        return _element_fac(EventSourceInstance,
                            j_event_source_inst,
                            event_source,
                            **{'definition': event_source_def})

    @_requires_scdb
    def onboard_exception(
            self,
            onboard_exception: Union[int, str]) -> OnboardException:
        """Lookup an onboard exception and return details about it

        :param onboard_exception: name or identifier of onboard exception to
            lookup

        :return: onboard exception description

        """
        if isinstance(onboard_exception, str):
            onboard_exception = self.name_to_onboard_exception_id(
                onboard_exception)
        if type(onboard_exception) is not int:
            raise TypeError(
                'onboard_exception(onboard_exception) '
                'requires onboard_exception to be str or int')

        j_exception = self.j_deployment_instance.getExceptions().getById(
            onboard_exception)
        if j_exception is None:
            raise TMTCModelQueryError(
                'Exception {} not present in deployment'.format(
                    onboard_exception))

        return _element_fac(OnboardException,
                            j_exception,
                            onboard_exception)

    @_requires_scdb
    def parameter_instance(self,
                           parameter: Union[int, str]) -> ParameterInstance:
        """Lookup a parameter instance and return details about its type

        :param parameter: name or identifier of parameter instance to lookup

        :return: parameter description

        """
        # Convert parameter name to ID
        if isinstance(parameter, str):
            parameter = self.name_to_parameter_id(parameter)
        if type(parameter) is not int:
            raise TypeError(
                'parameter_instance(parameter) '
                'requires parameter to be str or int')

        j_param_inst = self.j_deployment_instance.getParameters().getById(
            parameter)
        if j_param_inst is None:
            raise TMTCModelQueryError(
                'Parameter {} not present in deployment'.format(parameter))

        j_param_type = j_param_inst.getType()

        type_str = j_param_type.getTypeString()
        type_class = j_param_type.getTypeClass().toString()

        param_def = ParameterDefinition(
            signature=j_param_type.getSignature(),
            type_str=TypeStr[type_str.lower()],
            type_class=TypeClass[type_class.lower()],
            min_rows=j_param_type.getMinRows(),
            max_rows=j_param_type.getMaxRows(),
            bits_per_row=j_param_type.getBitsPerRow(),
            bytes_per_row=j_param_type.getBytesPerRow(),
            storage_bytes_per_row=j_param_type.getStorageBytesPerRow(),
            unused_bits_per_row=j_param_type.getUnusedBitsPerRow(),
            is_raw=j_param_type.isRaw(),
            is_fixed_size=j_param_type.isFixedSize(),
            is_read_only=j_param_type.isReadOnly(),
            is_config=j_param_type.isConfig()
        )

        return _element_fac(ParameterInstance,
                            j_param_inst,
                            parameter,
                            **{'definition': param_def})

    @_requires_scdb
    def parameter_instance_for_parameter_block(
            self,
            parameter_block,
            index_in_block: int) -> ParameterInstance:
        """Lookup a parameter within a block and return details about it

        :param parameter_block: name or identifier of parameter block to lookup
        :param index_in_block: index of parameter within block to lookup

        :return: parameter type description

        """
        if isinstance(parameter_block, str):
            parameter_block = self.name_to_parameter_block_id(parameter_block)
        if type(parameter_block) is not int:
            raise TypeError(
                'parameter_block_instance(parameter_block)'
                'requires parameter_block to be str or int')

        j_param_block_inst = \
            self.j_deployment_instance.getParameterBlocks().getById(
                parameter_block)
        if j_param_block_inst is None:
            raise TMTCModelQueryError(
                'Parameter Block {} not present in deployment'.format(
                    parameter_block))

        param_block_inst = ParameterBlockInstance(
            id=parameter_block,
            name=j_param_block_inst.getName(),
            full_name=j_param_block_inst.getFullName(),
            description=j_param_block_inst.getDescription(),
            documentation=Documentation(
                text=j_param_block_inst.getDocumentationAsText(),
                html=j_param_block_inst.getDocumentationAsHtml()),
            index=j_param_block_inst.getIndex(),
            definition=None
        )

        # We have to create a dummy as the model contains no information
        # on parameters within blocks
        param_def = ParameterDefinition(
            signature='',
            type_str=TypeStr.raw,
            type_class=TypeClass.var_vector,
            min_rows=1,
            max_rows=65535,
            bits_per_row=8,
            bytes_per_row=1,
            storage_bytes_per_row=1,
            unused_bits_per_row=0,
            is_raw=True,
            is_fixed_size=False,
            is_read_only=False,
            is_config=False
        )

        param_inst = ParameterInstance(
            id=parameter_block + index_in_block,
            name=param_block_inst.name + str(index_in_block),
            full_name=param_block_inst.full_name + str(index_in_block),
            description='A parameter from the ' +
                        param_block_inst.full_name +
                        ' parameter block',
            documentation=param_block_inst.documentation,
            index=index_in_block,
            definition=param_def
        )

        return param_inst

    def load_model_from_scdb(self, scdb_path: str):
        """Load the spacecraft database from file

        Passes filename of scdb to Java side to load and initialises the
        instance name lookup trees

        :param scdb_path: path to spacecraft database
        """
        self.j_model = \
            self._gw.j_model.Model(
                self._gw.j_model.JarModelReader(
                    scdb_path))
        self.j_deployment_instance = \
            self._gw.j_model.inst.DeploymentInst(
                self.j_model,
                self.j_model.getDeployment(''))

        self._initialise_lookup_trees()

    def _initialise_lookup_trees(self):
        # Construct python data structures with deployment element names
        self.parameter_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getParameters())
        self.parameter_block_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getParameterBlocks())
        self.action_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getActions())
        self.event_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getEvents())
        self.event_source_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getEventSources())
        self.onboard_exception_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getExceptions())
        self.component_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getComponents())
        self.component_group_name_lookup_tree = \
            self._init_name_lookup_tree(
                self.j_deployment_instance.getComponentGroups())

    @staticmethod
    def _init_name_lookup_tree(j_deployment_items):
        """ Construct tree for lookup of incomplete parameter names """
        item_lookup_tree = {}

        def name_and_id_tuple(j_item):
            # Create tuple of items reversed full name and items ID
            fully_qualified_name = j_item.getFullName()
            reverse_fqn = '.'.join(fully_qualified_name.split('.')[::-1])
            item_id = j_item.getId()
            return reverse_fqn, item_id

        # Don't support 'dotted'  names (i.e. datapool params)
        # This would be pretty complex to disambiguate from 'normal'
        # parameter names
        name_and_ids = [
            name_and_id_tuple(j_item) for
            j_item in j_deployment_items.getAllById().iterator()
            if '.' not in j_item.getName()
        ]

        # Sort by name depth
        name_and_ids.sort(key=lambda x: len(x[0].split('.')), reverse=True)

        for name_and_id in name_and_ids:
            _lookup_tree_put(item_lookup_tree, name_and_id[0], name_and_id[1])

        return item_lookup_tree

    @staticmethod
    def _name_to_id(name: str,
                    instance_elements,
                    name_lookup_tree) -> int:

        if not isinstance(name, str):
            raise TypeError('name must be a str')

        try:
            id_ = instance_elements.getByName(name).getId()
        except AttributeError:
            # element not found
            id_ = None

        if id_ is not None:
            return id_
        else:
            return ModelInspector._partial_name_to_id(
                name,
                name_lookup_tree)

    @staticmethod
    def _partial_name_to_id(name: str, lookup_tree) -> int:

        name_tree = _lookup_tree_get(lookup_tree, name)

        try:
            potential_matches = len(name_tree) if name_tree is not None else 0
        except TypeError:
            # Hit match immediately:
            return name_tree

        if potential_matches > 1:
            raise TMTCModelQueryError(
                'Name provided is ambiguous') from None

        elif potential_matches == 0:
            # No match found, raise error
            raise TMTCModelQueryError(
                'No matching name found') from None

        return _lookup_tree_get_id(name_tree)

    @staticmethod
    def _id_to_name(id_: int, instance_elements) -> str:
        if not isinstance(id_, int):
            raise TypeError('id must be an int')

        name = instance_elements.getById(id_).getFullName()

        if name:
            return name
        else:
            raise TMTCModelQueryError('No matching name found')


class _TransferListener:
    """Implements the Java TransferListener interface

    Attributes:
        completion_queue: Queue used to signal that the transfer is complete
        state_changed_callback: Callable to be called when the transfer state
                                has changed
        progress_callback: Callable to be called when the transfer progress
                           has changed

    """

    # ignore state parameter passed from Java
    # noinspection PyUnusedLocal,PyPep8Naming
    def stateChanged(self, transfer, state, stateDesc):
        """Called from Java side when a transfers state has changed"""

        logging.debug('Transfer State Changed: ' + stateDesc)

        # Call user provided callback
        if self.state_changed_callback:
            self.state_changed_callback(stateDesc)

        # Notify transfer has completed (enqueue error status code)
        if not transfer.isInProgress() or transfer.getError():
            self.completion_queue.put(transfer.getError())

    # ignore transfer parameter passed from Java
    # noinspection PyUnusedLocal,PyPep8Naming
    def progress(self, transfer, progressCount, progressTotal):
        """Called from Java side when a transfers progress has changed"""

        # Call user provided callback
        if self.progress_callback:
            self.progress_callback(
                progressCount,
                progressTotal)

    def equals(self, other):
        """Implements Java function"""
        return self == other

    class Java:
        implements = [
            'com.brightascension.gen1.protocol.transfer.TransferListener']

    def __init__(self, completion_queue,
                 state_change_callback: Callable[[str], None] = None,
                 progress_callback: Callable[[int, int], None] = None):
        self.completion_queue = completion_queue
        self.state_changed_callback = state_change_callback
        self.progress_callback = progress_callback


class _TMListener:
    """Abstract class that is used to register a set of listeners to be
    notified when unsolicited telemetry is received

    Listeners can be added or removed using the register and unregister
    functions, or by using the += and -= operators.

    The listeners attribute is a builtin set. Access the clear() method directly
    to remove all listeners.

    Attributes:
        listeners: Set of callables or queues that will be notified when
                   telemetry is received

    """

    def register(self, listener: Union[Callable, queue.Queue]):
        """Add a listener (callable or queue) to be notified when telemetry
        is received

        """
        self.listeners.add(listener)
        return self

    def unregister(self, listener: Union[Callable, queue.Queue]):
        """Remove a listener from the list of registered listeners"""

        try:
            self.listeners.remove(listener)
        except KeyError:
            raise ValueError(
                "Listener is not registered, so cannot unregister")
        return self

    def notify(self, *args):
        """Notify all registered listeners that telemetry has been received"""

        # Called asynchronously so we make a shallow copy snapshot here
        for listener in self.listeners.copy():
            try:
                listener(*args)
            except TypeError:
                # Not a callable- enqueue data
                listener.put_nowait(args)

    def equals(self, other):
        """Implements Java function"""
        return self == other

    def __init__(self):
        self.listeners = set()

    # Semantic sugar: add and remove listeners using += and -=
    __iadd__ = register
    __isub__ = unregister


class _EventListener(_TMListener):
    """Implements Java EventListener interface"""

    # Java method
    # noinspection PyPep8Naming
    def eventReceived(self, event_id_with_severity, source, info):
        """Called from Java side when an onboard event is received"""
        event_id = event_id_with_severity & ~0xC000

        event_severities = {0x0: 'info',
                            0x1: 'error',
                            0x2: 'component_fatal',
                            0x3: 'system_fatal'}

        severity = event_severities[event_id_with_severity >> 14]

        self.notify(event_id, severity, source, info)

    class Java:
        implements = [
            'com.brightascension.gen1.protocol.cmd.EventListener']


class _HkListener(_TMListener):
    """Implements the Java HkListener interface"""

    # Java method
    # noinspection PyPep8Naming
    def hkReceived(self, structure_id: int, data: bytes):
        """Called from Java side when a housekeeping packet is received"""
        self.notify(structure_id, data)

    class Java:
        implements = [
            'com.brightascension.gen1.protocol.cmd.HkListener']


class _DebugListener(_TMListener):
    """Implements the Java DebugListener interface"""

    # Java method
    # noinspection PyPep8Naming
    def debugReceived(self, debug_message: str):
        """Called from Java side when a debug message received"""
        self.notify(debug_message)

    class Java:
        implements = [
            'com.brightascension.gen1.protocol.cmd.DebugListener']


"""
 Classes for setting up space link to the onboard software
"""


class TCPServer:
    """ Manages a TCP Server that an onboard software instance will connect to

    """

    def __init__(self, port=51423, configuration=None):
        """
        :param port: port to listen for incoming connections on
        :param configuration: optional string describing configuration of
            protocol stack
            Set spacecraft id:           `SCID([1-9]+)`
            Use CCSDS Space Data Link:   `CCSDS_TM_DATALINK`
            Enable authentication layer: 'AUTH'

            Example value:
                'AUTH SCID1 CCSDS_TM_DATALINK'

        """

        self._port = port
        self._configuration = configuration
        self._j_link = None

    @property
    def port(self):
        return self._port

    @property
    def configuration(self):
        return self._configuration

    def connect(self, gateway: Py4JConnection):
        """Connect Space Link, and return command handler"""

        self._j_link = gateway.j_protocol.socket.ServerSocketWrapper()

        if self._configuration:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link, self._configuration)
        else:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link)

        command_handler = gateway.j_protocol.cmd.SyncCommandHandler(
            tmtc_services)

        self._j_link.connect(self._port)

        return command_handler

    def disconnect(self):
        self._j_link.disconnect()
        self._j_link = None

    def __str__(self):
        return 'TCPServer: port {}, {}'.format(
            self._port,
            'Connected' if self._j_link else 'Not Connected')


class TCPClient:
    """ Manages a TCP connection to an onboard software instance running a
    TCP Server

    """

    def __init__(self, host='127.0.0.1', port=51423, configuration=None):
        """
        :param host: host to connect to. IP address or Hostname
        :param port: port to connect to
        :param configuration: optional string describing configuration of
            protocol stack

            Set spacecraft id:           `SCID([1-9]+)`
            Use CCSDS Space Data Link:   `CCSDS_TM_DATALINK`
            Enable authentication layer: 'AUTH'

            Example: 'AUTH SCID1 CCSDS_TM_DATALINK'

        """

        self._configuration = configuration
        self._host = host
        self._port = port
        self._j_link = None

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def configuration(self):
        return self._configuration

    def connect(self, gateway: Py4JConnection):

        self._j_link = gateway.j_protocol.socket.ClientSocketWrapper()

        if self._configuration:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link, self._configuration)
        else:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link)

        command_handler = gateway.j_protocol.cmd.SyncCommandHandler(
            tmtc_services)

        self._j_link.connect(self.host, self._port)

        return command_handler

    def disconnect(self):
        if self._j_link is not None:
            self._j_link.disconnect()
            self._j_link = None

    def __str__(self):
        return 'TCPClient: host, {} port {}, {}'.format(
            self._host,
            self._port,
            'Connected' if self._j_link else 'Not Connected')


class UDP:
    """ Manages a UDP connection to an onboard software instance """

    def __init__(self,
                 host='127.0.0.1',
                 destination_port=51424,
                 source_port=51423,
                 configuration=None):
        """
        :param host: remote host. Destination IP address or Hostname
        :param destination_port: receiver's port
        :param source_port: sender's port
        :param configuration: optional string describing configuration of
            protocol stack

            Set spacecraft id:           `SCID([1-9]+)`
            Use CCSDS Space Data Link:   `CCSDS_TM_DATALINK`
            Enable authentication layer: 'AUTH'

            Example: 'AUTH SCID1 CCSDS_TM_DATALINK'

        """

        self._configuration = configuration
        self._host = host
        self._d_port = destination_port
        self._s_port = source_port
        self._j_link = None

    @property
    def host(self):
        return self._host

    @property
    def destination_port(self):
        return self._d_port

    @property
    def source_port(self):
        return self._s_port

    @property
    def configuration(self):
        return self._configuration

    def connect(self, gateway: Py4JConnection):

        self._j_link = gateway.j_protocol.socket.DatagramSocketWrapper()

        if self._configuration:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link, self._configuration)
        else:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link)

        command_handler = gateway.j_protocol.cmd.SyncCommandHandler(
            tmtc_services)

        self._j_link.connect(self.host, self._d_port, self._s_port)

        return command_handler

    def disconnect(self):
        if self._j_link is not None:
            self._j_link.disconnect()
            self._j_link = None

    def __str__(self):
        return 'UDP: host, {} dest port {}, source port {}, {}'.format(
            self._host,
            self.destination_port,
            self.source_port,
            'Connected' if self._j_link else 'Not Connected')


class Serial:
    """ Manages a Serial connection to an onboard software instance"""

    def __init__(self, device: str, baud_rate: int = 115200,
                 configuration: str = None):
        """
        :param device: serial device name to use, e.g. ttyUSB0
        :param baud_rate: baud rate, e.g 9600, 115200, etc
        :param configuration: optional string describing configuration of
            protocol stack

            Set spacecraft id:           `SCID([1-9]+)`
            Use CCSDS Space Data Link:   `CCSDS_TM_DATALINK`
            Enable authentication layer: 'AUTH'

            Example: 'AUTH SCID1 CCSDS_TM_DATALINK'

        """

        self.baud_rate = baud_rate
        self._configuration = configuration
        self._device = device
        self._j_link = None

    @property
    def device(self):
        return self._device

    @property
    def configuration(self):
        return self._configuration

    def connect(self, gateway: Py4JConnection):

        self._j_link = gateway.j_protocol.serial.SerialFrameLink()

        if self._configuration:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link, self._configuration)
        else:
            tmtc_services = gateway.j_protocol.cmd.DefaultProtocolStack(
                self._j_link)

        command_handler = gateway.j_protocol.cmd.SyncCommandHandler(
            tmtc_services)

        self._j_link.connect(self._device)
        self._j_link.setBaudRate(self.baud_rate)

        return command_handler

    def disconnect(self):
        self._j_link.disconnect()
        self._j_link = None

    def __str__(self):
        return 'Serial: device {}, {}'.format(
            self._device,
            'Connected' if self._j_link else 'Not Connected')


def _requires_tmtc_connection(method):
    """Decorator: ensure we don't attempt to send commands before we
    have set up connection to OBSW"""

    @functools.wraps(method)
    def wrapper(self, *method_args, **method_kwargs):
        if hasattr(self, '_command_handler') and self._command_handler:
            return method(self, *method_args, **method_kwargs)
        raise TMTCDisconnected('please run connect() first')

    return wrapper


class TMTCPy4j:
    """Exposes the GenerationOne TMTC API via an RPC connection using Py4J

    Send telecommands to, and receive telemetry from, an onboard software
    instance. Inspect the spacecraft database.

    Connects to a running instance of the onboard software using the connection
    settings passed in with the obsw_connection argument.

    This class can be used as a context manager - the connection to the
    onboard software will be started and stopped automatically.

    When actions/parameters/events etc. are referenced by name rather than id
    the name does not need to be fully qualified; the shortest unambiguous name
    that matches a name in the database can be used:

        >>> result = tmtc.get('platform.DummySubsys1.dummyParam32')
        >>> result = tmtc.get('dummyParam32')

    Attributes:
        default_timeout: default period to wait for a response to a command.

        event_listener: listens for event telemetry and notifies any registered
            callables/queues.
            Use the register_event_listener() and unregister_event_listener()
            functions, or the += and -= operators on this attribute.

        debug_listener: listens for debug telemetry and notifies any registered
            callables/queues.
            Use the register_debug_listener() and unregister_debug_listener()
            functions, or the += and -= operators on this attribute.

        hk_listener: listens for housekeeping telemetry and notifies any
            registered callables/queues.
            Use the register_hk_listener() and unregister_hk_listener()
            functions, or the += and -= operators on this attribute.

        model: provides methods used to query the spacecraft database.

    """

    def __init__(self,
                 classpath: str,
                 obsw_connection: Union[TCPServer, TCPClient, Serial],
                 scdb_filename: str,
                 default_timeout: int = 2000) -> None:
        """
        :param classpath: path to the jar file containing the TMTC API.
            This will be used to start a Java Virtual Machine that will
            persist until the last python thread has stopped.
        :param obsw_connection: configuration of the connection to the
            onboard software.
        :param scdb_filename: path to the spacecraft database (.scdb) file.
        :param default_timeout: default period to wait for a response to a
            telecommand

        """
        self._gw = Py4JConnection(classpath)

        self._obsw_connection = obsw_connection
        self._command_handler = None

        self._transfer_tmp = tempfile.TemporaryDirectory()

        self.event_listener = _EventListener()
        self.debug_listener = _DebugListener()
        self.hk_listener = _HkListener()

        self.default_timeout = default_timeout

        self.model = ModelInspector(self._gw, scdb_filename)

    # Methods related to performing TMTC commands against a
    # connected on-board software instance

    @_requires_tmtc_connection
    def query(self,
              parameter: Union[str, int],
              index_in_block=None,
              timeout: int = None) -> ParameterSize:
        """
        Query the dimensions of a parameter

        :param parameter: parameter name or id to be queried
        :param index_in_block: if this is a parameter in a block, e.g. a storage
            channel's contents, specify the index into the block to query
        :param timeout: if a response has not been received before this
            time (in ms) an exception will be raised.

        :return: ParameterSize tuple describing the dimensions of the parameter
                 (size: overall size in bytes, length: number of rows)

        """
        if timeout is None:
            timeout = self.default_timeout

        try:
            if index_in_block:
                parameter_block_instance = \
                    self.model.parameter_instance_for_parameter_block(
                        parameter,
                        index_in_block)
                j_param_size = self._command_handler.queryParameter(
                    parameter_block_instance.id,
                    timeout)
            else:
                parameter_instance = self.model.parameter_instance(
                    parameter)
                j_param_size = self._command_handler.queryParameter(
                    parameter_instance.id, timeout)

            return ParameterSize(size=j_param_size.getSize(),
                                 length=j_param_size.getLength())

        except Py4JJavaError as e:
            self.__handle_command_exception(e, 'Parameter query failed')

    @_requires_tmtc_connection
    def get(self,
            parameter: Union[str, int],
            first_row: int = 0,
            last_row: int = None,
            index_in_block=None,
            resize: bool = False,
            timeout: int = None):
        """
        Get the value of a parameter

        :param parameter: name or id of parameter
        :param first_row: index of the first row to get
        :param last_row: index of the last row to get
        :param index_in_block: index of parameter within parameter block to get
        :param resize: true if this should be a resizing get
            (Automatically adjust the last row requested if it is larger than
            the amount of rows present)
        :param timeout: if a response has not been received before this
            time (in ms) an exception will be raised

        :return: The parameter value. The type returned will be that of the
            parameter as specified in the spacecraft database

        """
        if timeout is None:
            timeout = self.default_timeout

        if index_in_block is not None:
            # This is a request to get a parameter that belongs to a
            # parameter block; we need to create a dummy parameter
            # instance definition to use with the API
            parameter_instance = \
                self.model.parameter_instance_for_parameter_block(
                    parameter,
                    index_in_block)
        else:
            parameter_instance = self.model.parameter_instance(parameter)

        param_def = parameter_instance.definition

        # Handle implied row ranges
        # Lookup parameter min/max rows
        if last_row is None:
            last_row = param_def.max_rows - 1
            resize = True
        if first_row > last_row:
            raise ValueError('first_row cannot be greater than last_row')

        try:
            if first_row == last_row:
                j_object = self._command_handler.getParameter(
                    parameter_instance.id,
                    first_row,
                    timeout)

                return self._from_gen1_type(j_object, param_def)

            else:
                j_object_list = self._command_handler.getParameter(
                    parameter_instance.id,
                    first_row,
                    last_row,
                    resize,
                    timeout)

                return [self._from_gen1_type(j_object, param_def) for j_object
                        in j_object_list]

        except Py4JJavaError as e:
            self.__handle_command_exception(e, 'Parameter get failed')

        except Py4JError as e:
            logging.error("Py4JError ", str(e))

    @_requires_tmtc_connection
    def set(self,
            parameter: Union[str, int],
            value,
            first_row: int = 0,
            index_in_block=None,
            resize: bool = False,
            timeout: int = None):
        """Set the value of a parameter

        :param  parameter: name or id of parameter.
        :param  value: new value to set parameter to. Use an iterable to set
            multiple rows.
        :param first_row: index of the first row to set.
        :param index_in_block: index of parameter within parameter block to set.
        :param resize: True if this should be a resizing set.
            (The size of the parameter will be adjusted such that the new last
            row of the parameter is the same as the last row specified)
        :param timeout: if a response has not been received before this
            time (in ms) an exception will be raised.

        """

        if timeout is None:
            timeout = self.default_timeout

        if index_in_block is not None:
            # This is a request to get a parameter that belongs to a parameter
            # block; we need to create a dummy parameter instance definition
            param_inst = \
                self.model.parameter_instance_for_parameter_block(
                    parameter,
                    index_in_block)
        else:
            param_inst = self.model.parameter_instance(
                parameter)

        if param_inst.definition.is_read_only:
            raise AttributeError("Cannot set read-only parameter")

        # Switch on parameter type class, convert parameter value provided
        # to parameter data (list of bytes)
        if param_inst.definition.type_class == TypeClass.var_raw:
            data = [self._to_gen1_type(value, param_inst.definition)]

        elif param_inst.definition.type_class in [TypeClass.value,
                                                  TypeClass.fixed_raw]:
            if isinstance(value, Iterable):
                raise TypeError(
                    'Cannot accept iterable for {} type class'.format(
                        param_inst.definition.type_class))
            data = [self._to_gen1_type(value, param_inst.definition)]

        elif param_inst.definition.type_class in [TypeClass.fixed_vector,
                                                  TypeClass.var_vector]:
            # Make list if value is not an Iterable already
            # (ie setting a single row)
            if not isinstance(value, Iterable):
                value = [value]

            # Don't try to validate row length for ParamBlock parameter:
            # no type data present in model
            validate_len = False if index_in_block else True

            data = [self._to_gen1_type(row,
                                       param_inst.definition,
                                       validate_len) for row in value]

        else:
            raise NotImplementedError('Unknown parameter TypeClass {}'.format(
                str(param_inst.definition.type_class)))

        # convert data (list of bytes) to list of
        # com.brightascension.gen1.util.ByteArray
        j_byte_arrays = [self._gw.j_util.ByteArray(row) for row in data]
        # to Java collection
        j_collection = self._gw.to_java_list(j_byte_arrays)

        try:
            self._command_handler.setParameter(
                param_inst.id,
                first_row,
                j_collection,
                resize,
                timeout)

        except Py4JJavaError as e:
            self.__handle_command_exception(e, 'Parameter set failed')

    @_requires_tmtc_connection
    def invoke(self, action: Union[str, int],
               argument=None,
               timeout: int = None):
        """
         Invoke an action

        :param action: name or ID of action to invoke.
        :param argument: optional argument value.
        :param timeout: if a response has not been received before this
            time (in ms) an exception will be raised

        """

        if timeout is None:
            timeout = self.default_timeout

        action_instance = self.model.action_instance(action)

        provided_arguments_count = 0 if argument is None else 1
        required_arguments_count = len(action_instance.definition.arguments)
        if provided_arguments_count != required_arguments_count:
            raise TMTCCommandError(
                '{} expects {} argument(s)'.format(
                    action_instance.full_name,
                    required_arguments_count))

        try:
            if argument:

                arg_details = self.model.argument(action, 0)

                if arg_details.is_fixed_size:
                    type_class = TypeClass.fixed_raw
                else:
                    type_class = TypeClass.var_raw

                param_def = ParameterDefinition(
                    signature='dummy signature',
                    type_str=TypeStr.raw,
                    type_class=type_class,
                    min_rows=arg_details.min_bytes,
                    max_rows=arg_details.max_bytes,
                    bits_per_row=8 * arg_details.min_bytes,
                    bytes_per_row=arg_details.min_bytes,
                    storage_bytes_per_row=1,
                    unused_bits_per_row=0,
                    is_raw=True,
                    is_fixed_size=arg_details.is_fixed_size,
                    is_read_only=False,
                    is_config=False
                )

                argument = self._to_gen1_type(argument, param_def)

                # to java object
                j_argument = self._gw.j_util.ByteArray(argument)

                self._command_handler.invokeAction(
                    action_instance.id,
                    j_argument,
                    timeout)

            else:
                self._command_handler.invokeAction(
                    action_instance.id,
                    timeout)

        except Py4JJavaError as e:
            self.__handle_command_exception(e, 'Action invoke failed')

    @_requires_tmtc_connection
    def uplink(self,
               parameter: Union[str, int],
               data,
               first_row: int = 0,
               index_in_block=None,
               resize: bool = False,
               max_retries=5,
               state_change_callback: Callable[[str], None] = None,
               progress_callback: Callable[[int, int], None] = None,
               timeout: int = None):
        """Uplink (up to 16384 bytes) to a parameter using multiple packets

        Set (and get) telecommands are limited to a single packet. For large
        data transfers, parameters will need to be uplinked.

        :param parameter: name or id of parameter.
        :param data: bytes-like data to be uplinked.
        :param first_row: row to start writing the data to.
        :param index_in_block: index of parameter within parameter block to
            uplink to.
        :param resize: whether this should be a resizing uplink.
            (The size of the parameter will be adjusted such that the new last
            row of the parameter is the same as the last row specified)
        :param max_retries: number of permitted retries.
        :param state_change_callback: Callable that will be notified when
            telemetry is received indicating that the transfer state has
            changed. Called with argument: state_description
        :param progress_callback: Callable that will be notified when
            telemetry is received indicating that the transfer progress has
            changed. Called with arguments: (progress_count, progress_total)
        :param timeout: if the transfer has not been completed before this
            period (in ms) expires teh transfer will be aborted and an
            exception raised.

        """
        if timeout is None:
            timeout = self.default_timeout

        if index_in_block is not None:
            # This is a request to set a parameter that belongs to a parameter
            # block; we need to create a dummy parameter instance definition
            parameter_instance = \
                self.model.parameter_instance_for_parameter_block(
                    parameter,
                    index_in_block)
        else:
            parameter_instance = self.model.parameter_instance(parameter)

        parameter_type = parameter_instance.definition

        if parameter_type.is_read_only:
            raise AttributeError("Cannot set read-only parameter")

        # We need to put the data to be uplinked into a file to pass to Java

        with tempfile.NamedTemporaryFile('wb', delete=False) as f:
            tempfile_name = f.name
            f.write(data)

        completion_q = queue.Queue(maxsize=1)
        transfer_listener = _TransferListener(
            completion_q,
            state_change_callback,
            progress_callback)

        last_row = (first_row + len(
            data) // parameter_type.bytes_per_row) - 1

        j_transfer = None
        try:
            j_transfer = self._command_handler.uplinkParameter(
                tempfile_name,
                parameter_instance.id,
                first_row,
                last_row,
                resize,
                max_retries,
                timeout,
                transfer_listener)

            exception_code = completion_q.get(timeout=timeout / 1000)

            if exception_code:
                exception_name = self.model.id_to_onboard_exception_name(
                    exception_code)
                exception_description = \
                    self.model.j_deployment_instance.getExceptions().getById(
                        exception_code).getDescription()
                raise TMTCTransferError(
                    'Exception occurred during transfer') from \
                    OBSWExceptionError(
                        exception_code,
                        exception_name,
                        exception_description)

        except Py4JJavaError as e:
            self.__handle_command_exception(e, 'Uplink failed')

        except queue.Empty as e:
            if j_transfer:
                j_transfer.abort()
            raise TMTCTransferError(
                'Transfer timed out (transfer aborted)') from e

        finally:
            os.remove(tempfile_name)

    @_requires_tmtc_connection
    def downlink(self,
                 parameter: Union[str, int],
                 first_row: int = 0,
                 last_row: int = None,
                 index_in_block=None,
                 resize: bool = False,
                 max_retries=10,
                 state_change_callback: Callable[[str], None] = None,
                 progress_callback: Callable[[int, int], None] = None,
                 timeout: int = None) -> bytes:
        """Downlink (up to 16384 bytes) from a parameter using multiple packets

        Get (and set) telecommands are limited to a single packet. For large
        data transfers, parameters will need to be downlinked

        :param parameter: name or id of parameter.
        :param first_row: row to start downlinking data from
        :param last_row: last row to downlink
        :param index_in_block: index of parameter within parameter block to
            downlink from.
        :param resize: True if this should be a resizing downlink.
            (Automatically adjust the last row requested if it is larger than
            the amount of rows present)
        :param max_retries: number of permitted retries.
        :param state_change_callback: Callable that will be notified when
            telemetry is received indicating that the transfer state has
            changed. Called with argument: state_description
        :param progress_callback: Callable that will be notified when
            telemetry is received indicating that the transfer progress has
            changed. Called with arguments: (progress_count, progress_total)
        :param timeout: if the transfer has not been completed before this
            period (in ms) expires teh transfer will be aborted and an
            exception raised.

        :return: the downlinked data

         """
        if timeout is None:
            timeout = self.default_timeout

        if index_in_block is not None:
            # This is a request to get a parameter that belongs to a parameter
            # block; We need to create a dummy parameter instance definition
            parameter_instance = \
                self.model.parameter_instance_for_parameter_block(
                    parameter,
                    index_in_block)
        else:
            parameter_instance = self.model.parameter_instance(
                parameter)

        parameter_type = parameter_instance.definition

        # Handle implied row ranges
        # Lookup parameter min/max rows
        if last_row is None:
            last_row = parameter_type.max_rows - 1
            resize = True
        if first_row > last_row:
            raise ValueError('first_row cannot be greater than last_row')

        # We need a file for the downlinked data. This is a limitation
        # of the Java API

        # noinspection PyProtectedMember
        downlinked_data_filename = os.path.join(
            self._transfer_tmp.name,
            next(tempfile._get_candidate_names()))

        j_file = self._gw.jvm.java.io.File(downlinked_data_filename)

        completion_q = queue.Queue(maxsize=1)

        transfer_listener = _TransferListener(
            completion_q,
            state_change_callback,
            progress_callback)

        transfer = self._command_handler.downlinkParameter(
            j_file,
            parameter_instance.id,
            first_row,
            last_row,
            parameter_type.bytes_per_row,
            resize,
            timeout,
            max_retries,
            transfer_listener)

        try:

            exception_code = completion_q.get(timeout=timeout / 1000)

            if exception_code:
                exception_name = self.model.id_to_onboard_exception_name(
                    exception_code)
                exception_description = \
                    self.model.j_deployment_instance.getExceptions().getById(
                        exception_code).getDescription()
                raise TMTCTransferError(
                    'Exception occurred during transfer') from \
                    OBSWExceptionError(
                        exception_code,
                        exception_name,
                        exception_description)

        except queue.Empty:
            os.remove(downlinked_data_filename)
            transfer.abort()
            raise TMTCTransferError('Transfer timed out') from None

        with open(downlinked_data_filename, 'rb') as f:
            downlinked_data = f.read()

        os.remove(downlinked_data_filename)

        return downlinked_data

    def register_event_listener(
            self,
            callback: Union[
                Callable[[int, int, int, bytes], None], queue.Queue]):
        """Add a Callable or Queue to the set of objects to be notified when
        event telemetry is received

        Callable or Queue will be called or notified with args tuple:
        (event_id, severity, source, info)

        :param callback: Callable or Queue to be added

        """
        self.event_listener += callback

    def unregister_event_listener(
            self,
            callback: Union[
                Callable[[int, int, int, bytes], None], queue.Queue]):
        """Remove a Callable or Queue from the set of objects to be notified
        when event telemetry is received

        :param callback: Callable or Queue to be removed

        """
        self.event_listener -= callback

    def register_housekeeping_listener(
            self,
            callback: Union[Callable[[int, bytes], None], queue.Queue]):
        """Add a Callable or Queue to the set of objects to be notified when
        event housekeeping is received

        Callable or Queue will be called or notified with args tuple:
        (structure_id, data)

        :param callback: Callable or Queue to be added

        """
        self.hk_listener += callback

    def unregister_housekeeping_listener(
            self,
            callback: Union[Callable[[int, bytes], None], queue.Queue]):
        """Remove a Callable or Queue from the set of objects to be notified
        when event housekeeping is received

        :param callback: Callable or Queue to be removed

        """
        self.hk_listener -= callback

    def register_debug_listener(
            self,
            callback: Union[Callable[[str], None], queue.Queue]):
        """Add a Callable or Queue to the set of objects to be notified when
        debug telemetry is received

        Callable or Queue will be called or notified with argument:
        debug_message

        :param callback: Callable or Queue to be added

        """
        self.debug_listener += callback

    def unregister_debug_listener(
            self,
            callback: Union[Callable[[str], None], queue.Queue]):
        """Remove a Callable or Queue from the set of objects to be notified
        when debug telemetry is received

        :param callback: Callable or Queue to be removed

        """
        self.debug_listener -= callback

    def connect(self):
        """Connect to the onboard software instance using the details provided
        at initialisation

        """
        if self._command_handler is None:
            self._command_handler = self._obsw_connection.connect(self._gw)
            self._command_handler.addEventListener(self.event_listener)
            self._command_handler.addDebugListener(self.debug_listener)
            self._command_handler.addHkListener(self.hk_listener)

    @_requires_tmtc_connection
    def disconnect(self):
        """Disconnect from the onboard software"""

        if self._command_handler is not None:
            self._command_handler.removeEventListener(self.event_listener)
            self._command_handler.removeDebugListener(self.debug_listener)
            self._command_handler.removeHkListener(self.hk_listener)
            self._command_handler = None
            self._obsw_connection.disconnect()

    def _from_gen1_type(self, java_object, param_def: ParameterDefinition):
        # We get a com.brightascension.gen1.util.ByteArray back from
        # Java side; lookup Parameter type, and return appropriately
        # represented value
        if param_def.type_str in [TypeStr.unsigned,
                                  TypeStr.bitfield]:

            if param_def.type_str == TypeStr.bitfield and \
                    param_def.bits_per_row == 1:
                # Special case, treat as boolean
                return java_object.getUnsignedValue() != 0
            else:
                return java_object.getUnsignedValue()

        elif param_def.type_str == TypeStr.signed:
            return java_object.getSignedValue()

        elif param_def.type_str == TypeStr.float:
            float_value = java_object.getBytes()
            return struct.unpack('f' if len(float_value) == 4 else 'd',
                                 java_object.getBytes())[0]

        elif param_def.type_str == TypeStr.parameterref:
            try:
                # Attempt lookup of parameter name
                return self.model.parameter_instance(
                    java_object.getUnsignedValue()).full_name
            except TMTCModelQueryError:
                return java_object.getUnsignedValue()

        else:  # Raw/Varaw
            return java_object.getBytes()

    def _to_gen1_type(self, value, parameter_type: ParameterDefinition,
                      validate_len=False) -> bytes:

        def twos_complement_range(n):
            _min = -(2 ** (n - 1))
            _max = 2 ** (n - 1) - 1
            return _min, _max

        try:
            # Special case, we accept parameter name strings for param refs
            if parameter_type.type_str == TypeStr.parameterref and type(
                    value) is str:
                value = self.model.name_to_parameter_id(value)

            if parameter_type.type_str in [TypeStr.parameterref,
                                           TypeStr.unsigned,
                                           TypeStr.bitfield]:
                return value.to_bytes(parameter_type.bytes_per_row, 'big',
                                      signed=False)

            elif parameter_type.type_str == TypeStr.signed:

                lower, upper = twos_complement_range(
                    parameter_type.bits_per_row)
                if not lower <= value <= upper:
                    raise ValueError(
                        "value outwith signed {}-bit range".format(
                            parameter_type.bits_per_row))

                return value.to_bytes(parameter_type.bytes_per_row, 'big',
                                      signed=True)

            elif parameter_type.type_str == TypeStr.float:
                return struct.pack(
                    'f' if parameter_type.bytes_per_row == 4 else 'd',
                    value)

            elif parameter_type.type_str in [TypeStr.raw, TypeStr.varaw]:
                if parameter_type.type_class == TypeClass.var_raw:
                    # Variable Raw
                    try:
                        memoryview(value)
                    except TypeError as e:
                        raise TypeError(
                            "for Varaw type parameters a "
                            "bytes-like object is expected, got {}".format(
                                type(value).__name__)) from e

                    if parameter_type.min_rows <= len(
                            value) <= parameter_type.bytes_per_row:
                        return value
                    else:
                        raise ValueError(
                            'len(value) not in range({}, {})'.format(
                                parameter_type.min_rows,
                                parameter_type.max_rows + 1))

                else:
                    # Fixed Raw
                    try:
                        # value is int, pad to row length
                        return value.to_bytes(parameter_type.bytes_per_row,
                                              'big', signed=False)
                    except AttributeError:
                        # value is already bytes, validate len
                        if len(value) is not parameter_type.bytes_per_row and \
                                validate_len:
                            raise ValueError(
                                'Provided bytearray wrong length. '
                                'Expected {}, got {}'.format(
                                    parameter_type.bytes_per_row,
                                    len(value))) from None
                        else:
                            # Value is already correct fixed size bytes
                            return value

        except (AttributeError, ValueError) as e:
            raise TypeError(
                'Cannot convert {} to Gen1 {} type'.format(
                    type(value).__name__, parameter_type.type_str), e) from e

    def __handle_command_exception(self, e, message=None):
        s = e.java_exception.toString()
        if s.startswith(
                'com.brightascension.gen1.protocol.cmd.SyncCommandException'):
            exception_code = e.java_exception.getException()
            exception_name = self.model.id_to_onboard_exception_name(
                exception_code)
            exception_description = \
                self.model.j_deployment_instance.getExceptions().getById(
                    exception_code).getDescription()
            logging.info('NACK: {}: {}'.format(exception_code, exception_name))
            raise OBSWExceptionError(exception_code, exception_name,
                                     exception_description) from None

        elif s.startswith(
                'com.brightascension.gen1.protocol.cmd.CommandException'):

            logging.error(
                message + ''
                          'Command Exception: '
                          ' OBSW instance may not be reachable.')

            raise TMTCCommandError(
                'Command failed ({}: {}). '
                'OBSW instance may not be reachable.'.format(
                    s.split(': ', 1)[1],
                    e.java_exception.getCause().toString().split(': ', 1)[1]
                    if e.java_exception.getCause() else ''
                )) from None

        elif s.startswith(
                'com.brightascension.gen1.protocol.transfer.TransferException'):
            raise TMTCTransferError(s.split(': ', 1)[1])

        else:
            logging.error(e.java_exception.toString())
            raise e

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        # make sure we disconnect the space link
        self.disconnect()

    def __del__(self):

        if getattr(self, '_command_handler', None):
            self.disconnect()

        # unlink transfers directory
        self._transfer_tmp.cleanup()
