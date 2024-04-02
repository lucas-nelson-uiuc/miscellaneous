import re
import json
import pandas as pd


def fix_mongodb_objects(line: str) -> str:
    """Resolve all MongoDB Object instances in JSON file.
    
    Python's JSON package does not allow for reading objects such as
    'ObjectId(...)' from a MongoDB extract. This script attempts to
    resolve these differences by:
        + reading the raw JSON file
        + replacing any MongoDB objects with only their values
    
    The output will be written to a JSON file for future processing.
    """

    def detect_mongodb_object(line: str) -> str:
        """Handle MongoDB Objects within a line.
        
        This function is responsible for the detecting if a line contains a
        MongoDB Object. Two situations arise:
            + if detected, pass the matched object to `replace_mongodb_object`
            + else, return the line

        The important part of this function is structuring the matched object -
        if there is a match - into a high-level dictionary. The re.Match object
        has a `groupdict` method containing all matched groups. These groups are
        defined as:
            + expression: the entire MongoDB Object
            + value: the value of the MongoDB Object

            Example: ObjectId("...") -> {'expression': ObjectId("..."), 'value':"..."}
        
        This is crucial for allowing `replace_mongodb_object` to successfully update
        the value of a MongoDB Object as intended.
        """

        RE_MONGODB_OBJECT = re.compile("(?P<expression>[A-Z]{1}[a-zA-Z]+\((?P<value>.*)\))")
        mongodb_object = RE_MONGODB_OBJECT.search(line)
        if isinstance(mongodb_object, re.Match):
            return replace_mongodb_object(mongodb_object=mongodb_object)
        return line
    
    
    def replace_mongodb_object(mongodb_object: re.Match) -> str:
        """Replace MongoDB Object with its value.

        The MongoDB Object provided is a regular expression Match object.
        This object contains all groups of matches, which `detect_mongodb_object`
        breaks into `expression` and `value`. We can replace the entire `expression`
        with only the `value`, effectively removing any MongoDB Object signature.
        
        Example: ObjectId("...") -> "..."
        """

        matches = mongodb_object.groupdict()
        return re.sub(re.escape(matches.get('expression')), matches.get('value'), line)
    
    return detect_mongodb_object(line)


def update_json_file(path: str, output_path: str) -> None:
    """Convert MongoDB export to reasonable JSON file"""

    with open(path) as fp:
        # read in lines, fix MongoDB objects
        lines = fp.readlines()
        lines_fixed = list(map(fix_mongodb_objects, lines))
        
        # handle writing data back to file
        with open(output_path, "w") as fp_write:
            for line in lines_fixed:
                fp_write.write(line)


def read_udpated_json_file(path: str) -> pd.DataFrame:
    """Load converted JSON file into memory"""

    with open(path) as fp_fixed:
        # read updated json file
        result = json.load(fp_fixed)
        # extract relevant rows from file
        row = {
            "customer_id": result.get("member").get("customer_id"),
            "rewards_amount": result.get("rewards").get("amount"),
            "rewards_currency": result.get("rewards").get("currency"),
            "grant_time": result.get("rewards_results")[0].get("grant_time")
        }
        # convert, store as pandas DataFrame
        data = pd.DataFrame.from_dict(row, orient='index')

    return data
