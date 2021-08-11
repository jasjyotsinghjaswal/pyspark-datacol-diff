"""
Defining Docstring for reusuable format and function
"""

import logging

# Define Error Message Template
err_msg = lambda lineno, exception_object: f"Error on line {lineno} & exception is {exception_object}"


def create_logger(logger_nm: str):
    """
    Function takes a logger name and returns a generic logger

    :param logger_nm: Name of the logger you want to return
    :return: Generic logger that can be reused
    """
    # Define Generic logger Variable
    gen_logger = logging.getLogger(logger_nm)
    gen_logger.setLevel(logging.DEBUG)
    # Create Stream Handler
    ch = logging.StreamHandler()
    # Create Formatter
    formatter = logging.Formatter('\n%(name)s - %(levelname)s - %(lineno)s - %(funcName)s - %(asctime)s - %(message)s')
    # Add formatter to ch
    ch.setFormatter(formatter)
    # Add ch to logger
    gen_logger.addHandler(ch)

    return gen_logger
