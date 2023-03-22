from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import logging
from enum import Enum
from typing import Any, List, Tuple
import uuid

import csv
import argparse

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()

# Partition strategy enum


class PartitionStrategy(Enum):
    RR = "Round_Robin"
    HASH = "Hash_Based"

# Custom tuple class with optional metadata


class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """

    def __init__(self, tuple, metadata=None, operator=None, location=[]):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator
        self.location = location

    def __repr__(self):
        return str(self.tuple)

    # Returns the lineage of self
    def lineage(self) -> List[ATuple]:
        return self.operator.lineage([self])[0]

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index) -> List[Tuple]:
        if isinstance(self.tuple, float) and att_index >= 1:
            return "ERROR: INDEX OUT OF RANGE. NO SUCH ATTRIBUTE AT THIS LOCATION."
        elif isinstance(self.tuple, tuple) and att_index >= len(self.tuple):
            return "ERROR: INDEX OUT OF RANGE. NO SUCH ATTRIBUTE AT THIS LOCATION."
        return self.operator.where(att_index, [self])[0]

    # Returns the How-provenance of self

    def how(self) -> str:
        return self.metadata

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        resp = self.operator.responsibility(self)
        return resp

# Data operator


class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    def __init__(self,
                 id=None,
                 name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        self.pull = pull
        self.partition_strategy = partition_strategy
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self) -> List[ATuple]:
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def apply(self, tuples: List[ATuple]) -> bool:
        logger.error("Apply method is not implemented! ")

# Scan operator


class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes scan operator

    def __init__(self,
                 filename,
                 filepath,
                 outputs: List[Operator],
                 is_left=True,
                 results=None,
                 k=100,
                 filter=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Scan, self).__init__(name="Scan",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.filename = filename
        self.filepath = filepath
        self.outputs = outputs
        self.filter = filter
        self.k = k
        self.offset = 1
        self.results = results
        self.data = None
        self.length = None
        self.is_left = is_left
        self.reader = self.read()
        self.line = 1

    def get_next(self):
        logger.info("Scanning tuples")
        try:
            return next(self.reader)
        except StopIteration:
            return None

    def read(self):
        output = []
        with open(self.filepath) as file:
            csvreader = csv.reader(file, delimiter=" ")
            next(csvreader)
            for curline in csvreader:
                curline = [int(elem) for elem in curline]
                prov = [(self.filename, self.line, tuple(curline), curline[i])
                        for i in range(len(curline))]
                how = 'f' + str(self.line) if self.filename[0] == "F" else 'r' + str(
                    self.line) + "@" + str(curline[-1])
                output.append(ATuple(tuple(curline), how, self, prov))
                self.line += 1
                if len(output) == self.k:
                    yield output
                    output = []
        yield output

    def lineage(self, tuples):
        return [[tup] for tup in tuples]

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        return [[tup.location[att_index]] for tup in tuples]

    # Starts the process of reading tuples (only for push-based evaluation)

    def start(self):
        logger.info("Applying Scan Operator")
        if len(self.outputs) != 0:
            nextOp = self.outputs[0]
        else:
            nextOp = None
        inputs = []
        with open(self.filepath, "r") as reader:
            reader.readline()
            for line in reader:
                # Once batch size is fulfilled, send out the batches
                if len(inputs) == self.k:
                    if isinstance(nextOp, Join):
                        nextOp.apply(inputs, self.is_left)
                    else:
                        if nextOp == None:
                            self.results += inputs
                        else:
                            nextOp.apply(inputs)
                    inputs.clear()
                splitList = line.split(" ")
                splitList = [int(elem) for elem in splitList]
                prov = [(self.filename, self.line, tuple(splitList), splitList[i])
                        for i in range(len(splitList))]
                how = 'f' + str(self.line) if self.filename[0] == "F" else 'r' + str(
                    self.line) + "@" + str(splitList[-1])
                inputs.append(ATuple(tuple(splitList), how, self, prov))
                self.line += 1
        # Send out remaining tuples in input
        if isinstance(nextOp, Join):
            nextOp.apply(inputs, self.is_left)
        else:
            if nextOp == None:
                self.results += inputs
            else:
                nextOp.apply(inputs)

    # Indicate to all operators that inputs are exhausted
    def finish(self):
        nextOp = self.outputs[0]
        if isinstance(nextOp, Join):
            nextOp.apply(None, None)
        else:
            nextOp.apply(None)

        # Returns the lineage of the given tuples

# Equi-join operator


class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_inputs (List): A list of handles to the instances of the operator
        that produces the left input.
        right_inputs (List):A list of handles to the instances of the operator
        that produces the right input.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes join operator

    def __init__(self,
                 left_inputs: List[Operator],
                 right_inputs: List[Operator],
                 outputs: List[Operator],
                 left_join_attribute,
                 right_join_attribute,
                 results=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Join, self).__init__(name="Join",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.left_inputs = left_inputs
        self.right_inputs = right_inputs
        self.outputs = outputs
        self.left_join_attribute = left_join_attribute
        self.right_join_attribute = right_join_attribute
        self.dictionary = {}
        self.left_dictionary = {}
        self.right_dictionary = {}
        self.lin_left = {}
        self.lin_right = {}
        self.results = results
    # Returns next batch of joined tuples (or None if done)

    def get_next(self):
        logger.info("Joining tuples")
        outputs = []
        op1 = self.left_inputs[0]
        op2 = self.right_inputs[0]
        if len(self.dictionary) == 0:
            self.set_dict(op1)
        rightInputs = op2.get_next()
        # Compare the current right batch with all the ones on the left side
        if rightInputs == None:
            return None
        for inps in rightInputs:
            results = inps.tuple
            key = results[self.right_join_attribute]
            if key in self.dictionary:
                res = self.dictionary[key]
                for match in res:
                    out = match.tuple + results
                    how = "(" + match.metadata + "*" + inps.metadata + ")"
                    if out not in self.lin_left:
                        self.lin_left[out] = [match]
                        self.lin_right[out] = [inps]
                    else:
                        self.lin_left[out].append(match)
                        self.lin_right[out].append(inps)
                    outputs.append(ATuple(out, how, self))
        return outputs

        # Returns the lineage of the given tuples

    # Get all the left side values
    def set_dict(self, op1):
        while True:
            results = op1.get_next()
            if results == None:
                break
            for result in results:
                tup = result.tuple
                if tup[self.left_join_attribute] not in self.dictionary:
                    self.dictionary[tup[self.left_join_attribute]] = [result]
                else:
                    self.dictionary[tup[self.left_join_attribute]].append(
                        result)

    def lineage(self, tuples):
        solution = []
        for tup in tuples:
            val = []
            for nextTupleLeft in self.lin_left[tup.tuple]:
                lin_left = nextTupleLeft.operator.lineage([nextTupleLeft])
                for left in lin_left:
                    val += left
            for nextTupleRight in self.lin_right[tup.tuple]:
                lin_right = nextTupleRight.operator.lineage([nextTupleRight])
                for right in lin_right:
                    val += right
            solution.append(val)
        return solution

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'

    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            key = self.lin_left[tup.tuple]
            if att_index >= len(key[0].tuple):
                att_index = att_index - len(key[0].tuple)
                key = self.lin_right[tup.tuple]
            for nextTuple in key:
                where = nextTuple.operator.where(att_index, [nextTuple])
                for li in where:
                    val += li
            whereProv.append(val)
        return whereProv

        return whereProv
    # Add tuples to the correct dictionary

    def add_to_dictionary(self, dictionary, inputs, index):
        for inp in inputs:
            tup = inp.tuple
            key = tup[index]
            if key in dictionary:
                dictionary[key].append(inp)
            else:
                dictionary[key] = [inp]

    # Compare tuples with the right dictionary
    def compare_dict(self, inputs, compareDict, indexInp, isLeft):
        res = []
        for inp in inputs:
            tup = inp.tuple
            key = tup[indexInp]
            if key in compareDict:
                value = compareDict[key]
                for val in value:
                    difTup = val.tuple
                    if isLeft == True:
                        out = tup + difTup
                        how = "(" + inp.metadata + "*" + val.metadata + ")"
                        if out not in self.lin_left:
                            self.lin_left[out] = [inp]
                            self.lin_right[out] = [val]
                        else:
                            self.lin_left[out].append(inp)
                            self.lin_right[out].append(val)
                        res.append(ATuple(out, how, self))
                    else:
                        out = difTup + tup
                        how = "(" + val.metadata + "*" + inp.metadata + ")"
                        if out not in self.lin_left:
                            self.lin_left[out] = [val]
                            self.lin_right[out] = [inp]
                        else:
                            self.lin_left[out].append(val)
                            self.lin_right[out].append(inp)
                        res.append(ATuple(out, how, self))
        return res

        # Applies the operator logic to the given list of tuples

    def apply(self, tuples: List[ATuple], isLeft: bool):
        logger.info("Applying Join Operator")
        # If all inputs exhausted
        if tuples == None:
            if self.results != None:
                return True
            else:
                return self.outputs[0].apply(None)
        # If left side
        if isLeft == True:
            res = self.compare_dict(
                tuples, self.right_dictionary, self.left_join_attribute, isLeft)
            self.add_to_dictionary(self.left_dictionary,
                                   tuples, self.left_join_attribute)
            if self.results != None:
                self.results += res
                return True if len(tuples) != 0 else False
            else:
                return self.outputs[0].apply(res)
        # If right side
        else:
            res = self.compare_dict(
                tuples, self.left_dictionary, self.right_join_attribute, isLeft)
            self.add_to_dictionary(self.right_dictionary,
                                   tuples, self.right_join_attribute)
            if self.results != None:
                self.results += res
                return True if len(tuples) != 0 else False
            else:
                return self.outputs[0].apply(res)


class Project(Operator):
    """Project operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes project operator

    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[None],
                 results=None,
                 fields_to_keep=[],
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Project, self).__init__(name="Project",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.field_to_keep = fields_to_keep
        self.results = results
        self.lineage_dictionary = {}
    # Return next batch of projected tuples (or None if done)

    def get_next(self):
        logger.info("Projecting tuples")
        outputs = []
        op = self.inputs[0]
        inputs = op.get_next()
        if inputs == None:
            return None
        for inp in inputs:
            tup = inp.tuple
            newList = [tup[i] for i in self.field_to_keep] if len(
                self.field_to_keep) > 0 else list(tup)
            out = tuple(newList)
            if str(out) in self.lineage_dictionary:
                self.lineage_dictionary[str(out)].append(inp)
            else:
                self.lineage_dictionary[str(out)] = [inp]
            outputs.append(ATuple(out, inp.metadata, self))
        return outputs
    # Returns the lineage of the given tuples

    def lineage(self, tuples):
        totalLineage = []
        for tup in tuples:
            val = []
            for nextTuple in self.lineage_dictionary[str(tup.tuple)]:
                lin = nextTuple.operator.lineage([nextTuple])
                for li in lin:
                    val += li
            totalLineage.append(val)
        return totalLineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'

    def responsibility(self, tup):
        new_tup = self.lineage_dictionary[str(tup.tuple)]
        return new_tup[0].operator.responsibility(new_tup)

    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            for nextTuple in self.lineage_dictionary[str(tup.tuple)]:
                ind = self.field_to_keep[att_index]
                where = nextTuple.operator.where(ind, [nextTuple])
                for li in where:
                    val += li
            whereProv.append(val)
        return whereProv

    # Applies the operator logic to the given list of tuples

    def apply(self, tuples: List[ATuple]):
        logger.info("Applying Project Operator")

        # If all inputs exhausted
        if tuples == None:
            if self.results != None:
                return True
            else:
                return self.outputs[0].apply(None)
        output = []
        for inp in tuples:
            tup = inp.tuple
            # Choose required indices
            newList = [tup[i] for i in self.field_to_keep] if len(
                self.field_to_keep) > 0 else list(tup)
            out = tuple(newList)
            if str(out) in self.lineage_dictionary:
                self.lineage_dictionary[str(out)].append(inp)
            else:
                self.lineage_dictionary[str(out)] = [inp]
            output.append(ATuple(out, inp.metadata, self))
        if self.results != None:
            self.results += output
            return True if len(tuples) != 0 else False
        else:
            return self.outputs[0].apply(output)

# Group-by operator


class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes average operator

    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 key,
                 value,
                 agg_gun,
                 results=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(GroupBy, self).__init__(name="GroupBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.value = value
        self.agg_gun = agg_gun
        self.dictionary = {}
        self.results = results
        self.lineage_dictionary = {}
        self.how_prov = {}

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        logger.info("Grouping tuples")
        outputs = []
        op = self.inputs[0]
        inputs = op.get_next()
        # Get all inputs
        if inputs == None:
            return None
        while True:
            res = op.get_next()
            if res == None:
                break
            inputs += res
        if inputs == []:
            return []
        # Apply grouping logic
        grouping = self.group(inputs)
        if self.key == self.value:
            val = self.agg_gun(list(grouping))
            outputs.append(
                ATuple((val), "AVG( " + self.how_prov["KeyVal"] + " )", self))
        else:
            for group in grouping:
                val = self.agg_gun(grouping[group])
                out = (group, val)
                outputs.append(
                    ATuple(out, "AVG( " + self.how_prov[group] + " )", self))
        # Return Groups
        return outputs

    def group(self, inputs):
        if self.key == self.value:
            res = []
            for inp in inputs:
                if "KeyVal" not in self.lineage_dictionary:
                    self.lineage_dictionary["KeyVal"] = [inp]
                    self.how_prov["KeyVal"] = inp.metadata
                else:
                    self.lineage_dictionary["KeyVal"].append(inp)
                    self.how_prov["KeyVal"] += ", " + inp.metadata
                res.append(inp.tuple[self.value])
            return res
        dictionary = {}
        for inp in inputs:
            tup = inp.tuple
            key = tup[self.key]
            if key in dictionary:
                self.lineage_dictionary[key].append(inp)
                dictionary[key].append(tup[self.value])
                self.how_prov[key] += ", " + inp.metadata
            else:
                self.lineage_dictionary[key] = [inp]
                dictionary[key] = [tup[self.value]]
                self.how_prov[key] = inp.metadata

        return dictionary

        # Returns the lineage of the given tuples

    def lineage(self, tuples):
        totalLineage = []
        for tup in tuples:
            if self.key == self.value:
                val = []
                for nextTuple in self.lineage_dictionary['KeyVal']:
                    lin = nextTuple.operator.lineage([nextTuple])
                    for li in lin:
                        val += li
                totalLineage.append(val)
            else:
                val = []
                for nextTuple in self.lineage_dictionary[tup.tuple[0]]:
                    lin = nextTuple.operator.lineage([nextTuple])
                    for li in lin:
                        val += li
                totalLineage.append(val)
        return totalLineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'

    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            if self.key == self.value:
                for nextTuple in self.lineage_dictionary["KeyVal"]:
                    ind = self.value
                    where = nextTuple.operator.where(ind, [nextTuple])
                    for li in where:
                        val += li
                whereProv.append(val)
            else:
                for nextTuple in self.lineage_dictionary[tup.tuple[0]]:
                    ind = self.value
                    where = nextTuple.operator.where(ind, [nextTuple])
                    for li in where:
                        val += li
                whereProv.append(val)
        return whereProv

    def groupify(self, inputs):
        for inp in inputs:
            tup = inp.tuple
            key = tup[self.key]
            val = tup[self.value]
            if self.key == self.value:
                if "KeyVal" not in self.dictionary:
                    self.lineage_dictionary["KeyVal"] = [inp]
                    self.dictionary["KeyVal"] = [val]
                    self.how_prov["KeyVal"] = inp.metadata
                else:
                    self.lineage_dictionary["KeyVal"].append(inp)
                    self.dictionary["KeyVal"].append(val)
                    self.how_prov["KeyVal"] += ", " + inp.metadata
            else:
                if key in self.dictionary:
                    self.lineage_dictionary[key].append(inp)
                    self.dictionary[key].append(val)
                    self.how_prov[key] += ", " + inp.metadata
                else:
                    self.lineage_dictionary[key] = [inp]
                    self.dictionary[key] = [val]
                    self.how_prov[key] = inp.metadata

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        output = []
        logger.info("Applying Group By Operator")
        # If all inputs are exhausted
        if tuples == None:
            for group in self.dictionary:
                val = self.agg_gun(self.dictionary[group])
                if group == "KeyVal":
                    output.append(
                        ATuple((val), "AVG( " + self.how_prov[group] + " )", self))
                else:
                    output.append(
                        ATuple((group, val), "AVG( " + self.how_prov[group] + " )", self))
            if self.results != None:
                self.results += output
                return True
            else:
                return self.outputs[0].apply(output)
        # Else, apply grouping logic
        else:
            self.groupify(tuples)
            return True


# Custom histogram operator


class Histogram(Operator):
    """Histogram operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes histogram operator

    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 key=0,
                 results=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Histogram, self).__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov,
                                        pull=pull,
                                        partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.results = results
        self.dictionary = {}
        self.lineage_dictionary = {}
        self.how_prov = {}

    # Returns histogram (or None if done)
    def get_next(self):
        logger.info("Creating Histogram")
        op = self.inputs[0]
        inputs = op.get_next()
        if inputs == None:
            return None
        while True:
            res = op.get_next()
            if res == None:
                break
            inputs += res
        # Do the logic
        outputs = []
        grouping = self.group(inputs)
        for group in grouping:
            val = grouping[group]
            out = (group, val)
            outputs.append(ATuple(out, self.how_prov[group], self))
        return outputs

    # Does main logic
    def group(self, inputs):
        dictionary = {}
        for inp in inputs:
            tup = inp.tuple
            key = tup[self.key]
            # Add one if already in dictionary otherwise initialize to one
            if key in dictionary:
                self.lineage_dictionary[key].append(inp)
                dictionary[key] += 1
                self.how_prov[key] += ", " + inp.metadata
            else:
                self.lineage_dictionary[key] = [inp]
                dictionary[key] = 1
                self.how_prov[key] = inp.metadata

        return dictionary

    def lineage(self, tuples):
        totalLineage = []
        for tup in tuples:
            val = []
            for nextTuple in self.lineage_dictionary[tup.tuple[0]]:
                lin = nextTuple.operator.lineage([nextTuple])
                for li in lin:
                    val += li
            totalLineage.append(val)
        return totalLineage

    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            for nextTup in self.lineage_dictionary[tup.tuple[0]]:
                where = nextTup.operator.where(self.key, [nextTup])
                for li in where:
                    val += li
            whereProv.append(val)
        return whereProv

    def groupify(self, inputs):
        for inp in inputs:
            tup = inp.tuple
            key = tup[self.key]
            if key in self.dictionary:
                self.lineage_dictionary[key].append(inp)
                self.dictionary[key] += 1
                self.how_prov[key] += ", " + inp.metadata
            else:
                self.lineage_dictionary[key] = [inp]
                self.dictionary[key] = 1
                self.how_prov[key] = inp.metadata

    # Applies the operator logic to the given list of tuples

    def apply(self, tuples: List[ATuple]):
        logger.info("Applying Histogram Operator")

        # If all inputs exhausted
        if tuples == None:
            if self.results != None:
                if len(self.results) == 0:
                    for key in self.dictionary:
                        val = self.dictionary[key]
                        out = (key, val)
                        self.results.append(
                            ATuple(out, self.how_prov[key], self))
                return True
            else:
                return self.outputs[0].apply()
        # Otherwise, apply histogram logic
        else:
            self.groupify(tuples)

# Order by operator


class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes order-by operator

    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 comparator,
                 results=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(OrderBy, self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.comparator = comparator
        self.results = results
        self.lineage_dictionary = {}
        self.how_provenance = {}
        self.responsible = None
        self.friend_responsible = {}
        self.rating_responsible = {}
    # Returns the sorted input (or None if done)

    def get_next(self):
        logger.info("Ordering tuples")
        op = self.inputs[0]
        inputs = op.get_next()
        if inputs == None:
            return None
        while True:
            res = op.get_next()
            if res == None:
                break
            inputs += res
        tupList = self.filter_to_tuple(inputs)
        orderedList = self.comparator(tupList)
        outputs = [ATuple(inp, self.how_provenance[inp], self)
                   for inp in orderedList]
        self.calculate_responsibility(outputs)
        return outputs

    # Simply makes ATuples into Tuples
    def filter_to_tuple(self, inputs):
        tups = []
        for inp in inputs:
            if inp.tuple not in self.lineage_dictionary:
                self.lineage_dictionary[inp.tuple] = [inp]
                self.how_provenance[inp.tuple] = inp.metadata

            else:
                self.lineage_dictionary[inp.tuple].append(inp)
            tups.append(inp.tuple)
        return tups

    def calculate_responsibility(self, tuples):
        parsed_tups, friend_tups, rating_tups = self.parse_tuples(tuples)
        friends = self.responsibility(friend_tups, rating_tups, parsed_tups, 0)
        ratings = self.responsibility(rating_tups, friend_tups, parsed_tups, 1)
        self.responsible = friends + ratings
        return parsed_tups

    def responsibility(self, list_1, list_2, parsed, is_friend):
        res = []
        for new_val in list_1:
            new_parsed = self.remove_instances(parsed, is_friend, new_val)
            average = self.calculate_average(new_parsed)
            set_flag = 0
            if average[0] != max(average):
                res.append((self.friend_responsible[new_val], 1) if is_friend == 0 else (
                    self.rating_responsible[new_val], 1))
                continue
            else:
                for dif_val in list_1:
                    if dif_val == new_val:
                        continue
                    else:
                        final_parsed = self.remove_instances(
                            new_parsed, is_friend, dif_val)
                        new_average = self.calculate_average(final_parsed)
                        if new_average[0] != max(new_average):
                            res.append((self.friend_responsible[new_val], 0.5) if is_friend == 0 else (
                                self.rating_responsible[new_val], 0.5))
                            set_flag = 1
                            break
                if set_flag == 1:
                    continue
                for fin_val in list_2:
                    final_parsed = self.remove_instances(
                        new_parsed, (is_friend + 1) % 2, fin_val)
                    new_average = self.calculate_average(final_parsed)
                    if new_average[0] != max(new_average):
                        res.append((self.friend_responsible[new_val], 0.5) if is_friend == 0 else (
                            self.rating_responsible[new_val], 0.5))
                        break
        return res

    def remove_instances(self, tuples, is_friend, val_rem):
        instances = []
        for tup in tuples:
            unmatched = [val for val in tup if val[is_friend] != val_rem]
            instances.append(unmatched)
        return instances

    def calculate_average(self, tuples):
        averages = []
        for tup in tuples:
            if len(tup) == 0:
                avg = 0
            else:
                summed = sum(z for x, y, z in tup)
                avg = summed / len(tup)
            averages.append(avg)
        return averages

    def parse_tuples(self, tuples):
        parsed = []
        unraveled_friends = []
        unraveled_ratings = []
        seen_f = {}
        seen_r = {}
        for tup in tuples:
            i = 0
            lin = tup.lineage()
            metadata = tup.metadata
            split = metadata.split("AVG( ")[1]
            split = split.split(" )")[0]
            split = split.split(", ")
            how = []
            for how_tup in split:
                friend_tup = lin[i].tuple
                rating_tup = lin[i + 1].tuple
                i += 2
                new_split = how_tup.split("(")[1]
                new_split = new_split.split(")")[0]
                new_split = new_split.split("*")
                friend_ind = int(new_split[0][1:])
                rating_split = new_split[1].split("@")
                rating_ind = int(rating_split[0][1:])
                rating = int(rating_split[1])
                how.append((friend_ind, rating_ind, rating))
                if friend_ind not in seen_f:
                    self.friend_responsible[friend_ind] = friend_tup
                    seen_f[friend_ind] = True
                    unraveled_friends.append(friend_ind)
                if rating_ind not in seen_r:
                    self.rating_responsible[rating_ind] = rating_tup
                    seen_r[rating_ind] = True
                    unraveled_ratings.append(rating_ind)
            parsed.append(how)
        return parsed, unraveled_friends, unraveled_ratings
    # Returns the lineage of the given tuples

    def lineage(self, tuples):
        totalLineage = []
        for tup in tuples:
            val = []
            for nextTuple in self.lineage_dictionary[tup.tuple]:
                lin = nextTuple.operator.lineage([nextTuple])
                for li in lin:
                    val += li
            totalLineage.append(val)
        return totalLineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'

    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            for nextTup in self.lineage_dictionary[tup.tuple]:
                where = nextTup.operator.where(att_index, [nextTup])
                for li in where:
                    val += li
            whereProv.append(val)
        return whereProv

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        logger.info("Applying Order By Operator")
        # Apply predicate
        filtered = self.filter_to_tuple(tuples)
        orderedList = self.comparator(filtered)
        output = [ATuple(inp, self.how_provenance[inp], self)
                  for inp in orderedList]
        self.calculate_responsibility(output)
        if self.results != None:
            self.results += output
            return True
        else:
            return self.outputs[0].apply(output)


# Top-k operator


class TopK(Operator):
    """TopK operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes top-k operator

    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 results=None,
                 k=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(TopK, self).__init__(name="TopK",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.k = k
        self.results = results
        self.lineage_dictionary = {}

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        logger.info("Getting top tuples")
        outputs = []
        op = self.inputs[0]
        inputs = op.get_next()
        if inputs == None:
            return None
        while True:
            res = op.get_next()
            if res == None:
                break
            inputs += res
        self.set_lin(inputs)
        if self.k == None:
            return [ATuple(inp.tuple, inp.metadata, self) for inp in inputs]
        # Apply TopK logic
        if inputs == []:
            outputs = []
        else:
            outputs = [ATuple(inputs[i].tuple, inputs[i].metadata, self)
                       for i in range(self.k)]
        return outputs

    def set_lin(self, tuples):
        for tup in tuples:
            if tup.tuple not in self.lineage_dictionary:
                self.lineage_dictionary[tup.tuple] = [tup]
            else:
                self.lineage_dictionary[tup.tuple].append(tup)

    # Returns the lineage of the given tuples

    def lineage(self, tuples):
        totalLineage = []
        for tup in tuples:
            val = []
            for nextTup in self.lineage_dictionary[tup.tuple]:
                lin = nextTup.operator.lineage([nextTup])
                for li in lin:
                    val += li
            totalLineage.append(val)
        return totalLineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            for nextTup in self.lineage_dictionary[tup.tuple]:
                where = nextTup.operator.where(att_index, [nextTup])
                for li in where:
                    val += li
            whereProv.append(val)
        return whereProv

    def responsibility(self, tup):
        next_tup = self.lineage_dictionary[tup[0].tuple]
        return next_tup[0].operator.responsible

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        logger.info("Applying Top K Operator")
        self.set_lin(tuples)
        if tuples == []:
            output = []
        else:
            output = [ATuple(tuples[i].tuple, tuples[i].metadata, self)
                      for i in range(self.k)]
        if self.results != None:
            self.results += output
            return True
        else:
            return self.outputs[0].apply(output)

# Filter operator


class Select(Operator):
    """Select operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes select operator

    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 predicate,
                 is_left=True,
                 results=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Select, self).__init__(name="Select",
                                     track_prov=track_prov,
                                     propagate_prov=propagate_prov,
                                     pull=pull,
                                     partition_strategy=partition_strategy)
        self.inputs = inputs
        self.outputs = outputs
        self.predicate = predicate
        self.results = results
        self.is_left = is_left
        self.lineage_dictionary = {}

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        logger.info("Selecting tuples")
        outputs = []
        op = self.inputs[0]
        inputs = op.get_next()
        if inputs == None:
            return None
        # Selects indices required
        for tup in inputs:
            getTuple = tup.tuple
            if self.predicate(getTuple) == True:
                if getTuple not in self.lineage_dictionary:
                    self.lineage_dictionary[getTuple] = [tup]
                else:
                    self.lineage_dictionary[getTuple].append(tup)
                outputs.append(ATuple(getTuple, tup.metadata, self))
        return outputs

    def lineage(self, tuples: List[ATuple]):
        totalLineage = []
        for tup in tuples:
            val = []
            for nextTup in self.lineage_dictionary[tup.tuple]:
                lin = nextTup.operator.lineage([nextTup])
                for li in lin:
                    val += li
            totalLineage.append(val)
        return totalLineage

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        whereProv = []
        for tup in tuples:
            val = []
            for nextTup in self.lineage_dictionary[tup.tuple]:
                where = nextTup.operator.where(att_index, [nextTup])
                for li in where:
                    val += li
            whereProv.append(val)
        return whereProv

        # Applies the operator logic to the given list of tuples

        # Push Based Select

    def apply(self, tuples: List[ATuple]):
        logger.info("Applying Select Operator")
        # If all inputs are exhauseted
        if tuples == None:
            # If join
            if self.results == None:
                if isinstance(self.outputs[0], Join):
                    return self.outputs[0].apply(None, None)
                else:
                    return self.outputs[0].apply(None)
            else:
                return True
        output = []
        # Apply select logic
        for val in tuples:
            tup = val.tuple
            if self.predicate(tup) == True:
                if tup in self.lineage_dictionary:
                    self.lineage_dictionary.append(val)
                else:
                    self.lineage_dictionary[tup] = [val]
                output.append(ATuple(tup, val.metadata, self))
        if self.results != None:
            self.results += output
            return True if len(tuples) != 0 else False
        else:
            if isinstance(self.outputs[0], Join):
                return self.outputs[0].apply(output, self.is_left)
            else:
                return self.outputs[0].apply(output)


class Sink(Operator):
    def __init__(self,
                 left_outputs=List[Operator],
                 right_outputs=List[Operator],
                 k=100,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Sink, self).__init__(name="TopK",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.left_outputs = left_outputs
        self.right_outputs = right_outputs
        self.k = k

    def begin(self):
        # Begin Left Tuples for Push Based
        logger.info("Beginning Left")
        self.left_outputs[0].start()
        # Begin Right Tuples for Push Based
        if self.right_outputs != None:
            logger.info("Beginning Right")
            self.right_outputs[0].start()
        # Finish
        self.left_outputs[0].finish()


def average(x): return sum(x) / len(x)
def tuplistfunc(x): return sorted(x, key=lambda tup: tup[1], reverse=True)


file_name1 = 'Friends.csv'
file_name2 = 'Ratings.csv'

# Query 1: Push Based


def q1(filepath1, filepath2, uid, mid, k, pushList):
    groupby = [GroupBy([], [], 4, 4, average, pushList)]
    join = [Join([], [], groupby, 1, 0)]
    select1 = [Select([], join, lambda x: x[0] == uid)]
    select2 = [Select([], join, lambda x: x[1] == mid, False)]
    scan1 = [Scan(file_name1, filepath1, select1, True,  None, k)]
    scan2 = [Scan(file_name2, filepath2, select2, False, None, k)]
    sink = Sink(scan1, scan2)
    sink.begin()
    return pushList

# Query 2: Push Based


def q2(filepath1, filepath2, uid, k, pushList):
    project = [Project([], [], pushList, [0])]
    topk = [TopK([], project, None, 1)]
    orderby = [OrderBy([], topk, tuplistfunc)]
    groupby = [GroupBy([], orderby, 0, 1, average)]
    project = [Project([], groupby, None, [3, 4])]
    join = [Join([], [], project, 1, 0)]
    select = [Select([], join, lambda x: x[0] == uid)]
    scan1 = [Scan(file_name1, filepath1, select, True, None, k)]
    scan2 = [Scan(file_name2, filepath2, join, False, None, k)]
    sink = Sink(scan1, scan2)
    sink.begin()
    return pushList

# Query 3: Push Based


def q3(filepath1, filepath2, uid, mid, k, pushList):
    histogram = [Histogram([], [], 4, pushList)]
    join = [Join([], [], histogram, 1, 0)]
    select1 = [Select([], join, lambda x: x[0] == uid)]
    select2 = [Select([], join, lambda x: x[1] == mid, False)]
    scan1 = [Scan(file_name1, filepath1, select1, True, None, k)]
    scan2 = [Scan(file_name2, filepath2, select2, False, None, k)]
    sink = Sink(scan1, scan2)
    sink.begin()
    return pushList

# Query 1: Pull Based


def query1(filepath1, filepath2, uid, movieid, k):
    scan1 = [Scan(file_name1, filepath1, [], None, k)]
    scan2 = [Scan(file_name2, filepath2, [], None, k)]
    select1 = [Select(scan1, [], lambda x: x[0] == uid)]
    select2 = [Select(scan2, [], lambda x: x[1] == movieid)]
    join = [Join(select1, select2, [], 1, 0)]
    groupby = GroupBy(join, [], 4, 4, average)
    return groupby.get_next()

# Query 2: Pull Based


def query2(filepath1, filepath2, uid, k):
    scan1 = [Scan(file_name1, filepath1, [], None,  k)]
    scan2 = [Scan(file_name2, filepath2, [], None, k)]
    select = [Select(scan1, [], lambda x: x[0] == uid)]
    join = [Join(select, scan2, [], 1, 0)]
    groupby = [GroupBy(join, [], 3, 4, average)]
    orderby = [OrderBy(groupby, [], tuplistfunc)]
    topk = [TopK(orderby, [], None, 1)]
    project = Project(topk, [], None, [0])
    return project.get_next()

# Query 3 : Pull Based


def query3(filepath1, filepath2, uid, movieid, k):
    scan1 = [Scan(file_name1, filepath1, [], k)]
    scan2 = [Scan(file_name2, filepath2, [], None, k)]
    select1 = [Select(scan1, [], lambda x: x[0] == uid)]
    select2 = [Select(scan2, [], lambda x: x[1] == movieid)]
    join = [Join(select1, select2, [], 1, 0)]
    histogram = Histogram(join, [], 4)
    return histogram.get_next()


if __name__ == "__main__":

    logger.info("Assignment #1")

    parser = argparse.ArgumentParser()
    parser.add_argument('--query', dest="queryNum", type=int,
                        help="Get Query Number", required=True)
    parser.add_argument('--ff', dest="file1", type=str,
                        help="Get Friend File", required=True)
    parser.add_argument('--mf', dest="file2", type=str,
                        help="Get Rating File", required=True)
    parser.add_argument('--uid', dest="userId", type=int,
                        help="Get UserId", required=True)
    parser.add_argument('--mid', dest="movieId", type=int,
                        help="Get Movie Id", required=False)
    parser.add_argument('--pull', dest="pull", type=int,
                        help="Push or pull", required=False)
    parser.add_argument('--output', dest="outputFile", type=str,
                        help="Destination for output file", required=True)
    parser.add_argument('--lineage', dest="lineage", type=int,
                        help="Get lineage of tuple", required=False)
    parser.add_argument('--where-row', dest="where_row",
                        help="Tuple for where provenance", type=int, required=False)
    parser.add_argument('--where-attribute', dest="where_attribute",
                        type=int, help="Attribute for where")
    parser.add_argument('--how', dest="how", type=int,
                        help="Tuple to get how provenance from", required=False)
    parser.add_argument('--responsibility', dest="responsibility",
                        type=int, help="Tuple to get responsibility", required=False)
    args = parser.parse_args()
    argDictionary = vars(args)

    queryNum = argDictionary['queryNum']
    file1 = argDictionary['file1']
    file2 = argDictionary['file2']
    userId = argDictionary['userId']
    movieId = argDictionary['movieId']
    pull = argDictionary['pull']
    outputFile = argDictionary['outputFile']
    lineage = argDictionary['lineage']
    where_row = argDictionary['where_row']
    where_attribute = argDictionary['where_attribute']
    how = argDictionary['how']
    responsibility = argDictionary['responsibility']

    k = 500
    pushList = []
    if queryNum == 1:
        if pull == 1:
            output = query1(file1, file2, userId, movieId, k)
        else:
            output = q1(file1, file2, userId, movieId, k, pushList)
    elif queryNum == 2:
        if pull == 1:
            output = query2(file1, file2, userId, k)
        else:
            output = q2(file1, file2, userId, k, pushList)
    else:
        if pull == 1:
            output = query3(file1, file2, userId, movieId, k)
        else:
            output = q3(file1, file2, userId, movieId, k, pushList)

    if lineage != None:
        try:
            textFile = output[lineage].lineage()
        except IndexError:
            textFile = "INDEX OUT OF RANGE. TUPLE DOES NOT EXIST AT THIS LOCATION"
    elif where_row != None:
        try:
            textFile = output[where_row].where(where_attribute)
        except IndexError:
            textFile = "INDEX OUT OF RANGE. TUPLE DOES NOT EXIST AT THIS LOCATION"
    elif how != None:
        try:
            textFile = output[how].how()
        except IndexError:
            textFile = "INDEX OUT OF RANGE. TUPLE DOES NOT EXIST AT THIS LOCATION"
    elif responsibility != None:
        try:
            textFile = output[responsibility].responsible_inputs()
        except IndexError:
            textFile = "INDEX OUT OF RANGE. TUPLE DOES NOT EXIST AT THIS LOCATION"
    else:
        textFile = "NO TASK SELECTED. PLEASE SPECIFY BETWEEN LINEAGE, WHERE, HOW, and RESPONSIBILITY"
    tfile = open(outputFile, "w")
    tfile.write(str(textFile))
    tfile.close()

    logger.info("Assignment #2")
