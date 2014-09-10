<?php

/**
* The main library. Just an autoloader, actually.
*
* Copyright (C) 2011 Bogdan Stancescu <bogdan@moongate.ro>
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see {@link http://www.gnu.org/licenses/}.
*
* @package php-zookeeper-recipes
* @author Bogdan Stăncescu <bogdan@moongate.ro>
* @version 1.0
* @copyright Copyright (c) October 2012, Bogdan Stăncescu
* @license http://www.gnu.org/licenses/agpl-3.0.html GNU Affero GPL
*/

function ZR_autoloader($class_name)
{
	$fname=dirname(__FILE__)."/classes/".$class_name.".php";
	if (!is_readable($fname)){
		return false;
    }

	include $fname;
	return true;
}

spl_autoload_register('ZR_autoloader',false);

