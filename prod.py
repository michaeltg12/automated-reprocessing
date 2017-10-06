#!/Users/ofg/anaconda3/bin/python

from sympy import sympify, symbol
from matplotlib import pyplot as plt
import io_methods
import argparse
import logging
import netCDF4
import time
import os
import re

module_path = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("reprocessing")

# ####################### default input for testing #######################
# # path to folder which contains file with list and files to be changed
# path = '/Users/ofg/Desktop/Reprocessing/archive_data/nsaskyrad60sC1.b1/'
# # file = 'nsaskyrad60sC1.b1.19990401.000000.cdf'  # old ... for changing one file at a time
# file_list = 'nsaskyrad_files.txt'
# # list of use cases for file reprocessing tasks
# # equation = 'base_time = 1999-3-31 19:0'
# # equation = 'psp1_mean = psp1_mean + 42'
# # equation = 'psp1_mean = psp1_mean * 3'
# # equation = 'psp1_mean = psp1_mean ** 2'
# # equation = 'psp1_mean = psp1_sd + 42'
# # equation = 'psp1_mean = psp1_sd + psp1_mean'
# equation = 'base_time = 1999-3-31 19:0  & psp1_mean=psp1_mean/5 & psp1_mean = psp1_mean + 42 & psp1_max = psp1_mean*2'
# #########################################################################

def get_args():
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('reproc_file', type=str,
		default='/Users/ofg/Desktop/Reprocessing/archive_data/nsaskyrad60sC1.b1/nsaskyrad_files.txt',
		help='full path to file containing a list of files to be reprocessed')
	parser.add_argument('equations', type=str,
		default='base_time = 1999-3-31 19:0  & psp1_mean=psp1_mean/5 & psp1_mean = psp1_mean + 42 & psp1_max = psp1_mean*2',
		help='equations separated by "&" symbol with one left side argument followed by an "="')

	testing = parser.add_mutually_exclusive_group(required=False)
	testing.add_argument('-t', default=False, action='store_true', help='flag for testing')

	return parser.parse_args()

class Equation(object):
    """ Equation object for passing all attributes

    Author: Michael Giansiracusa
    Email: giansiracumt@ornl.gov

    Purpose:
        hold all necessary attributes of an equation
        so they can all be passed together.

    Attributes:
        left_side (str):
        right_side (str):
        right_vars (list)
        symbolic_expr (sympy.core.add.Add):

    """
    def __init__(self, equation: str) -> None:
        self.equation = equation
        self.equation_list = None
        self.left_side = None
        self.right_side = None
        self.right_vars = None
        self.symbolic_expr = None

    def assign_attributes(self):
        self.equation = self.equation_preprocessing()
        split_equ = self.equation.split('=')  # split equa into left and right sides based on '=' sign
        left_side = split_equ[0].strip()  # strip left side of leading or trailing spaces
        right_side = split_equ[1].strip()  # strip right side of leading or trailing spaces
        right_vars = re.findall('[a-zA-Z1]+[_]?[1]?[a-zA-Z]+', right_side)
        self.equation_list = split_equ
        self.left_side = left_side
        self.right_side = right_side
        self.right_vars = right_vars
        self.symbolic_expr = self.parse_string_2_symbolic_expr()  # parse the equation to a symbolic expression

    def equation_preprocessing(self):
        # this method checks for special cases
        # such as 'base_time = 1999-3-31 19:0'
        # in this case the time needs to be changed
        # into a timestamp format such as
        # 'base_time = 922924800'
        equa = self.equation
        logger.info('equa: %s'%equa)
        # assert len(equa) > 0, 'Equation empty'
        # assert isinstance(equa, str), 'Equation is not a string'
        # assert re.search('=', equa), 'Equation format: %s' % equa
        # logger.info('All parse equ input checks passed')
        time_regex = 'time'
        if re.search(time_regex, equa):
            date_regex = '[0-9]{2,4}-[0-9]{1,2}-[0-9]{1,2} [1-9]{1,2}:[0-9]{1,2}'
            date = re.findall(date_regex, equa)
            for date_element in date:
                timestamp = self.date_to_timestamp(date_element)
                equa = re.sub(date_element, timestamp, equa)

        logger.info("return from eq_preproc: '%s'" % equa)
        return equa

    def date_to_timestamp(self, date_element):
        try:
            return str(int(time.mktime(time.strptime(date_element, '%Y-%m-%d %H:%M'))))
        except:
            return time.mktime(time.strptime(input('enter date time (year-month-day hour:min) \
                                                   \nWhere hour between 0-23: '), '%Y-%m-%d %H:%M'))

    def parse_string_2_symbolic_expr(self):
        # assertions moved to apply_expr(equa) method
        equa = self.equation

        # search for variables based on regex
        # also may not be necessary with controlled variable selection
        regex = '[a-zA-Z1]+[_]?[1]?[a-zA-Z]+'
        variables = set(re.findall(regex, equa))  # return a set of all variables found using regex

        syms = symbol.var(equa)  # parse whole equation to a tuple of symbols

        right_side = equa.split('=')[1].strip()  # split equation on '=', take second piece, strip leading/trailing spaces
        expr = sympify(right_side)  # using sympy method sympify, parse right side to symbolic expression
        # ex) ( atm + aos ) * 2 --> 2*aos + 2*atm where (aos, atm) are symbols
        # note: sympy multiplication is communicable but symbols are not

        # return symbolic expression of right side,
        # set of variables used ( used for debugging, may not be necessary with proper symbols)
        # tuple of symbols parsed from right side of symbolic expression.
        #return expr, set(variables), syms
        return expr

def apply_expr(equa: str, rootgrp: netCDF4._netCDF4.Dataset):  # equa is an equation of the form a = b where b can be a composite expression

    left_side, right_side, right_vars = equa.left_side, equa.right_side, equa.right_vars
    expr = equa.symbolic_expr

    orig = rootgrp.variables[left_side]  # for review and reporting
    proc_level = 0

    if len(equa.right_vars) == 0:  # execute if there are no variables on the right side
        logger.info('single substitution')  # for debugging
        # simple substitution ie. change serial # or calibration coefficient
        rootgrp.variables[equa.left_side] = eval(equa.right_side)

    elif len(equa.right_vars) >= 1:  # if there are variables on the right side then do this
        logger.info('iterative substitution')  # for debugging
        temp = []  # temp holds changed values before setting rootgrp.variables[ left_side ] = temp

        # execute this loop if the variable on the left side is present on the right side
        # this means the equation has a form like 'psp1_mean = psp1_mean + 42'
        # *** this is too general, possible implementation error when more use cases added ***
        if len(equa.right_vars) == 1 and equa.left_side == equa.right_vars[0].strip():
            proc_level = 1
            logger.info('interative constant change')  # for debugging
            for element in rootgrp.variables[equa.left_side][:]:  # iterate over all elements of 1D array from variables

                # using symbolic expression (expr) substitue each element from rootgrp.variables[left_side]
                # in for the variable to change by a constant ammount and append to temp array
                temp.append(expr.subs(equa.left_side, element))

                # execute this loop if the variable on the left side is not present on the right side
        # this means the equation has a form like 'psp1_mean = psp1_sd + 42'
        # *** this is too general, possible implementation error when more use cases added ***
        elif len(equa.right_vars) == 1:
            proc_level = 2
            logger.info('iterative change based on secondary variable')  # for debugging
            right_var = equa.right_vars[0].strip()  # get the first variable present on the right and strip extra spaces
            logger.info('right_var: %s' %right_var)
            for element in rootgrp.variables[right_var][:]:  # iterate over all elements of 1D array from variables

                # using symbolic expression (expr) substitue each element from rootgrp.variables[right_var]
                # in for the variable to change by a constant ammount and append to temp array
                temp.append(expr.subs(right_var, element))

        elif len(equa.right_vars) > 1:
            proc_level = 2
            logger.info('iterative change based on 2 secondary variables')  # for debugging

            # iterate over all elements of multople 1D arrays from variables using index range
            # *** assertion that the each array has the same number of elements ****
            # *** this could be a problem if there are holes in data ***
            for index in range(len(rootgrp.variables[right_vars[0].strip()][:])):
                temp_expr = expr  # temp expression for iterative substituion
                for i in range(len(equa.right_vars)):  # iterate over list of variables on the right side of expression
                    # substitute the value for each variable from the rootgrp into the expression
                    # at similar index value iteratively and resave the evaluated expression at each step
                    # at the end the expression has no variables left and is evaluated as an Integer or Float
                    temp_expr = temp_expr.subs(equa.right_vars[i], rootgrp.variables[equa.right_vars[i].strip()][index])

                temp.append(temp_expr)  # save the evaluated Integer or Float into temp array

        # set the variable from the left side equal to the temp array of values generated above
        rootgrp.variables[left_side] = temp

    # return variable changed from the left side of the equation for debugging and plotting
    return left_side, orig, proc_level, right_vars

def verify_file(args):
    if os.path.isfile(args.reproc_file):
        verified_file = args.reproc_file
    else:
        verified_file = '/Users/ofg/Desktop/Reprocessing/archive_data/nsaskyrad60sC1.b1/nsaskyrad_files.txt'
    return verified_file

def do_reprocessing(verified_file, args, io_manager):
    with open(verified_file) as files:
        file_counter = 0  # for debugging and reporting
        # equation_counter = 1  # for separating plots by using 'plt.figure(equation_counter)'

        for file in files:

            #directory = os.path.dirname(os.path.realpath(args.reproc_file))
            directory = '/Users/ofg/Desktop/Reprocessing/archive_data/nsaskyrad60sC1.b1/'

            path2file = os.path.join(directory, file)  # create path name and open netCDF Dataset below
            logger.info('path2file: %s' %path2file)
            rootgrp = netCDF4.Dataset(path2file.strip(), "r+", format="NETCDF4")
            equation_list = args.equations.split('&')

            for equa in equation_list:

                output_struct = {}
                equation_obj = Equation(equa)
                equation_obj.assign_attributes()
                try:
                    output_struct['original'] = rootgrp.variables[equation_obj.left_side][:]
                except:
                    output_struct['original'] = rootgrp.variables[equation_obj.left_side]

                kwargs = {'equa': equation_obj, 'rootgrp': rootgrp}  # key word args, for stability

                # apply equation to dataset and return left hand side (ls) of the equation
                # original value of data set before change (og)
                # processing level for plotting and reporting (pl)
                # and right side variable names for plotting and reporting (rv)
                ls, og, pl, rv = apply_expr(**kwargs)  # *** this is where the work happens ***

                try:
                    output_struct['reprocessed'] = rootgrp.variables[equation_obj.left_side][:]
                except:
                    output_struct['reprocessed'] = rootgrp.variables[equation_obj.left_side]
                output_struct['equation'] = equa
                for var in equation_obj.right_vars:
                    output_struct[str(var)] = rootgrp.variables[var][:]

            out_file = (file.strip() + str(file_counter))
            logger.info('out_file: %s' % out_file)

            io_manager.save_obj(output_struct, out_file)

            #     if re.search('time', equa):
            #         logger.info('time substitution\n%s from:\n%s\t\tchanged to\n----> %s\n' % (ls, og, rootgrp.variables[ls]))
            #
            #     elif pl == 0:
            #         logger.info('equation counter: %s' % equation_counter)  # for debugging
            #         plt.figure(equation_counter)  # create figure for debugging
            #         # plot below for showing replacement change where equation form like: 'base_time = 200'
            #         plt.title('replacement change (%s)' % equa)
            #         logger.info('plotting %s = %s' % (ls, rootgrp.variables[ls]))
            #         plt.plot(rootgrp.variables[ls], 'go', label='new %s' % ls)
            #         plt.plot(og, 'r.', label='old %s' % ls)
            #         plt.legend()
            #
            #     elif pl == 1:
            #         logger.info('equation counter: %s' % equation_counter)
            #         plt.figure(equation_counter)  # create figure for debugging
            #         # plot below for showing iterative constant change where equation form like 'psp1_mean = psp1_mean + 200'
            #         plt.title('iterative constant change(%s)' % equa)
            #         plt.plot(rootgrp.variables[ls][:], 'g-', label='new %s' % ls)
            #         plt.plot(og, 'r:', label='old %s' % ls)
            #         plt.legend()
            #
            #     elif pl == 2:
            #         logger.info('equation counter: %s' % equation_counter)
            #         plt.figure(equation_counter)  # create figure for debugging
            #         # plot below for showing iterative change where equation form like 'psp1_mean = psp1_sd + 200'
            #         plt.title('iterative variable change(%s)' % equa)
            #         plt.plot(rootgrp.variables[ls][:], 'g-', label='new %s' % ls)
            #         plt.plot(rootgrp.variables[rv[0]][:], 'b:', label='aux var %s' % rv[0])
            #         plt.plot(og, 'r:', label='old %s' % ls)
            #         plt.legend()
            #
            #     logger.info('\n##### Finished with equ: %s #####\n\n' %(equa))
            #     equation_counter += 1
            # logger.info('##### plotting changes #####')
            # plt.show()  # show plot
            #
            file_counter += 1
            if file_counter > 2:
                break

if __name__ == '__main__':

    args = get_args()
    verified_file = verify_file(args)

    input_dir = module_path + '/input/'
    output_dir =module_path + '/output/'
    logger.debug('\n\tin: %s\n\tout: %s' %(input_dir, output_dir))
    io_manager = io_methods.IOMethods(input_dir, output_dir, logging.DEBUG)

    do_reprocessing(verified_file, args, io_manager)