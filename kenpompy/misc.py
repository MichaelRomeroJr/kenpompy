"""
This module provides functions for scraping the miscellaneous stats kenpom.com pages into more
usable pandas dataframes.
"""

import mechanicalsoup
import pandas as pd
from bs4 import BeautifulSoup
import pathlib
import re

def pom_rating_name_clean_up(dataframe, year=None):
	for index in dataframe.index:
		iter_team = dataframe.loc[index,'Team']
		iter_conf = dataframe.loc[index,'Conference']

		try:
			if iter_team[-3:] == "St.":
				dataframe.loc[index,'Team'] = dataframe.loc[index,'Team'][:-1] + "ate"
				
			if iter_team == 'Miami OH':	
				dataframe.loc[index,'Team'] = 'Miami (OH)'
			if iter_team == "Miami FL":	
				dataframe.loc[index,'Team'] = "Miami (FL)"
			if iter_team == "St. Francis PA":	
				dataframe.loc[index,'Team'] = "St. Francis (PA)"
			if iter_team == "Bryant":	
				dataframe.loc[index,'Team'] = "Bryant University"
			if iter_team == "Southern":	
				dataframe.loc[index,'Team'] = "Southern University"
			if iter_team == "Bethune Cookman":	
				dataframe.loc[index,'Team'] = "Bethune-Cookman"
			if iter_team == "Cal Baptist":	
				dataframe.loc[index,'Team'] = "California Baptist"	
			if iter_team == "The Citadel":
				dataframe.loc[index,'Team'] = "Citadel"	
			if iter_team == "Central Connecticut":
				dataframe.loc[index,'Team'] = "Central Connecticut State"
			if iter_team == "Loyola MD":
				dataframe.loc[index,'Team'] = "Loyola (MD)"
			if iter_team == "Cal St. Bakersfield":	
				dataframe.loc[index,'Team'] = "Cal State Bakersfield"
			if iter_team == "LIU":	
				dataframe.loc[index,'Team'] = "LIU Brooklyn"
			if iter_team == "NJIT":	
				dataframe.loc[index,'Team'] = "N.J.I.T."
			if iter_team == "Louisiana Monroe":	
				dataframe.loc[index,'Team'] = "Louisiana-Monroe"
			if iter_team == "Illinois Chicago":	
				dataframe.loc[index,'Team'] = "Illinois-Chicago"
			if iter_team == "Gardner Webb":	
				dataframe.loc[index,'Team'] = "Gardner-Webb"
			if iter_team == "Nicholls St.":	
				dataframe.loc[index,'Team'] = "Nicholls State"
			if iter_team == "Cal St. Northridge":	
				dataframe.loc[index,'Team'] = "Cal State Northridge"
			if iter_team == "UNC Wilmington":	
				dataframe.loc[index,'Team'] = "North Carolina-Wilmington"
			if iter_team == 'Mississippi':	
				dataframe.loc[index,'Team'] = 'Ole Miss'
			if iter_team == 'Tennessee Martin':	
				dataframe.loc[index,'Team'] = 'Tennessee-Martin'
			if iter_team == 'SIU Edwardsville':	
				dataframe.loc[index,'Team'] = 'SIU-Edwardsville'
			if iter_team == 'Montana St.':	
				dataframe.loc[index,'Team'] = 'Montana State'
			if iter_team == 'Nebraska Omaha':	
				dataframe.loc[index,'Team'] = 'Nebraska-Omaha'
			# if iter_team == "Long Beach":
			# 	dataframe.loc[index,'Team'] = "Long Beach State"	

			# Changes unique dataframe values of kenpom.com/index 
			if iter_team == "Saint Mary" and iter_conf == "WCC":
				dataframe.loc[index,'Team'] = "Saint Mary's"
			if iter_team == "North Carolina Central " and iter_conf == "MEAC":
				dataframe.loc[index,'Team'] = "North Carolina Central"
			if iter_team == "Boston University " and iter_conf == "Pat":
				dataframe.loc[index,'Team'] = "Boston University"
			if iter_team == "Texas A&M Corpus Chris" and iter_conf == "Slnd":
				dataframe.loc[index,'Team'] = "Texas A&M-CC"
			if iter_team == "Maryland Eastern Shore" and iter_conf == "MEAC":
				dataframe.loc[index,'Team'] = "Maryland-Eastern Shore"
			if iter_team == "St. Francis NY" and iter_conf == "NEC": 	
				dataframe.loc[index,'Team'] = "St. Francis (BKN)"
			if iter_team == "Saint Joseph's" and iter_conf == "A10": 	
				dataframe.loc[index,'Team'] = "Saint Joseph's (PA)"
			if iter_team == "Saint Peter's" and iter_conf == "MAAC": 	
				dataframe.loc[index,'Team'] = "St. Peter's"
			if iter_team == "Cal St. Fullerton" and iter_conf == "BW": 	
				dataframe.loc[index,'Team'] = "Cal State Fullerton"
			if iter_team == "Arkansas Pine Bluff" and iter_conf == "SWAC":
				dataframe.loc[index,'Team'] = "Arkansas-Pine Bluff"
			if iter_team == "College of Charleston":
				dataframe.loc[index,'Team'] = "Charleston"
	
			# 11/11 Update
			if 	iter_team == "Fort Wayne" or iter_team == "IPFW":
				dataframe.loc[index,'Team'] = "Purdue Fort Wayne"	
			if 	iter_team == "Arkansas Little Rock":
				dataframe.loc[index,'Team'] = "Little Rock"		
			if 	iter_team == "Texas Pan American":
				dataframe.loc[index,'Team'] = "UT Rio Grande Valley"
			if iter_team == "Louisiana Lafayette":
				dataframe.loc[index,'Team'] = "Louisiana"
			if iter_team == "College of Charleston":
				dataframe.loc[index,'Team'] = "Charleston"
			if iter_team == "NJIT": 
				dataframe.loc[index,'Team'] = "N.J.I.T."
		except:
			pass
		pass
	return dataframe





def are_teams_unique(dataframe):
	test_list = dataframe['Team'].tolist()

	all_unique = len(set(test_list)) == len(test_list) 

	seen = set()
	uniq = [x for x in test_list if x not in seen and not seen.add(x)]   
	seen = {}
	dupes = []

	for x in test_list:
		if x not in seen:
			seen[x] = 1
		else:
			if seen[x] == 1:
				dupes.append(x)
			seen[x] += 1
	print('kenpom.com/index.php duplicates: ', dupes)

	if len(dupes) == 0:
		return True
	else:
		return False

def get_pomeroy_ratings(browser, season=None):
	"""
	Scrapes the Pomeroy College Basketball Ratings table (https://kenpom.com/index.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.
		season (str, optional): Used to define different seasons. 2002 is the earliest available season.
			Most recent season is the default.
	Returns:
		refs_df (pandas dataframe): Pandas dataframe containing the Pomeroy College Basketball Ratings table from kenpom.com.
	Raises:
		ValueError: If `season` is less than 2002.
	"""
	url = 'https://kenpom.com/index.php'
	if season and int(season) < 2002:
		raise ValueError("season cannot be less than 2002")
	url += '?y={}'.format(season)
	browser.open(url)
	page = browser.get_current_page()
	table = page.find_all('table')[0]
	
	ratings_df = pd.read_html(str(table))

	if season==2020 or season==2021: # has astrick in column 
		tmp_df_names = ratings_df[0].iloc[:,1].str.replace('\d+\*+', '').str.rstrip() 	 
	else:
		tmp_df_names = ratings_df[0].iloc[:,1].str.replace('\d+', '').str.rstrip() 

	tmp_df_ranks =  ratings_df[0].iloc[:,1].str.extract(r"(\d+)")
	tmp_df_names = tmp_df_names.rename("Team")

	test_list = tmp_df_names.tolist()

	#all_unique = len(set(test_list)) == len(test_list) 

	# Dataframe tidying.
	ratings_df = ratings_df[0]
	ratings_df.columns = ratings_df.columns.map(lambda x: x[1])

	for index in ratings_df.index:
		try:
			iter_team = ratings_df.loc[index,'Team']
			# format the team name 
			if iter_team[-1] == '*': # if it ends in '*'
				iter_team = iter_team.replace('*', '')

				# for character in iter_team: # remove rank 
				# 	if character.isdigit():
				# 		iter_team = iter_team.replace(character, '')

			for character in iter_team: # remove rank 
				if character.isdigit():
					iter_team = iter_team.replace(character, '')
			
			if iter_team[-1] == ' ': # if it ends in ' '
				iter_team = iter_team[:-1]

			ratings_df.loc[index,'Team'] = iter_team
		except Exception as e:
			
			path_str = pathlib.Path(__file__).parent.absolute()
			#print('Error: ',  path_str, '\n', e)
			
			pass
	
	# Parse out seed, most current won't have this
	#tmp = ratings_df['Team'].str.extract('(?P<Team>[a-zA-Z]+\s*[a-zA-Z]+\.*)\s*(?P<Seed>\d*)')
		
	ratings_df["Team"] = tmp_df_names
	ratings_df["Seed"] = tmp_df_ranks
	
	#  This method removed slices of the df 
	#df = ratings_df[ratings_df.Team != 'Team']
	#df = df.drop(df[df['Team']==0].index).dropna()
	
	# rename columns for database 
	ratings_df.columns = ['Rank', 'Team', 'Conference', 'win_loss', 'adjusted_efficiency_margin', 
		'adjusted_offense', 'adjusted_offense_rank', 'adjusted_defense', 'adjusted_defense_rank', 
		'adjusted_tempo', 'adjusted_tempo_rank', 'luck', 'luck_rank', 
		'SoS_adjusted_efficiency_margin', 'SoS_adjusted_efficiency_margin_rank', 
		'SoS_OppO', 'SoS_OppO_rank','SoS_OppD', 'SoS_OppD_rank', 
		'NCSOS_adjusted_efficiency_margin', 'NCSOS_adjusted_efficiency_margin_rank', 'Seed']

	if are_teams_unique(dataframe=ratings_df): 
		#update_df = pom_rating_name_clean_up(dataframe=df, year=season)
		update_df = pom_rating_name_clean_up(dataframe=ratings_df, year=season)

	#ratings_df = pom_rating_name_clean_up(ratings_df, year=season)
	else: # the non-unique ones are the column headers  
		update_df = pom_rating_name_clean_up(dataframe=ratings_df, year=season)
	
	return update_df #ratings_df


def get_trends(browser):
	"""
	Scrapes the statistical trends table (https://kenpom.com/trends.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.

	Returns:
		trends_df (pandas dataframe): Pandas dataframe containing the statistical trends table from kenpom.com.
	"""

	url = 'https://kenpom.com/trends.php'

	browser.open(url)
	trends = browser.get_current_page()
	table = trends.find_all('table')[0]
	trends_df = pd.read_html(str(table))

	# Dataframe tidying.
	trends_df = trends_df[0]
	trends_df.drop(trends_df.tail(5).index, inplace=True)

	return trends_df


def get_refs(browser, season=None):
	"""
	Scrapes the officials rankings table (https://kenpom.com/officials.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.
		season (str, optional): Used to define different seasons. 2016 is the earliest available season.
			Most recent season is the default.

	Returns:
		refs_df (pandas dataframe): Pandas dataframe containing the officials rankings table from kenpom.com.

	Raises:
		ValueError: If `season` is less than 2016.
	"""

	url = 'https://kenpom.com/officials.php'

	if season:
		if int(season) < 2016:
			raise ValueError(
				'season cannot be less than 2016, as data only goes back that far.')
		url = url + '?y=' + str(season)

	browser.open(url)
	refs = browser.get_current_page()
	table = refs.find_all('table')[0]
	refs_df = pd.read_html(str(table))

	# Dataframe tidying.
	refs_df = refs_df[0]
	refs_df.columns = ['Rank', 'Name', 'Rating', 'Games', 'Last Game', 'Game Score', 'Box']
	refs_df = refs_df[refs_df.Rating != 'Rating']
	refs_df = refs_df.drop(['Box'], axis=1)

	return refs_df


def get_hca(browser):
	"""
	Scrapes the home court advantage table (https://kenpom.com/hca.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.
		season (str, optional): Used to define different seasons. 2010 is the earliest available season.

	Returns:
		hca_df (pandas dataframe): Pandas dataframe containing the home court advantage table from kenpom.com.
	"""

	url = 'https://kenpom.com/hca.php'

	browser.open(url)
	hca = browser.get_current_page()
	table = hca.find_all('table')[0]
	hca_df = pd.read_html(str(table))

	# Dataframe tidying.
	hca_df = hca_df[0]
	# hca_df.columns = ['Team', 'Conference', 'HCA', 'HCA.Rank', 'PF', 'PF.Rank', 'Pts', 'Pts.Rank', 'NST',
	# 					'NST.Rank', 'Blk', 'Blk.Rank', 'Elev', 'Elev.Rank']

	# 12/11 clean up
	hca_df.columns = ['Team', 'Conference', 
		'HCA', 'HCA_rank', 'PF', 'PF_rank', 'Pts', 'Pts_rank', 'NST',
		'NST_rank', 'Blk', 'Blk_rank', 'Elev', 'Elev_rank']
	hca_df = hca_df[hca_df.Team != 'Team']

	return hca_df


def get_arenas(browser, season=None):
	"""
	Scrapes the arenas table (https://kenpom.com/arenas.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.
		season (str, optional): Used to define different seasons. 2010 is the earliest available season.
			Most recent season is the default.

	Returns:
		arenas_df (pandas dataframe): Pandas dataframe containing the arenas table from kenpom.com.

	Raises:
		ValueError: If `season` is less than 2010.
	"""

	url = 'https://kenpom.com/arenas.php'

	if season:
		if int(season) < 2010:
			raise ValueError(
				'season cannot be less than 2010, as data only goes back that far.')
		url = url + '?y=' + str(season)

	browser.open(url)
	arenas = browser.get_current_page()
	table = arenas.find_all('table')[0]
	arenas_df = pd.read_html(str(table))

	# Dataframe tidying.
	arenas_df = arenas_df[0]
	arenas_df.columns = ['Rank', 'Team', 'Conference', 'Arena', 'Alternate']
	arenas_df['Arena'], arenas_df['Arena.Capacity'] = arenas_df['Arena'].str.split(' \(').str
	arenas_df['Arena.Capacity'] = arenas_df['Arena.Capacity'].str.rstrip(')')
	arenas_df['Alternate'], arenas_df['Alternate.Capacity'] = arenas_df['Alternate'].str.split(' \(').str
	arenas_df['Alternate.Capacity'] = arenas_df['Alternate.Capacity'].str.rstrip(')')

	return arenas_df


def get_gameattribs(browser, season=None, metric='Excitement'):
	"""
	Scrapes the Game Attributes tables (https://kenpom.com/game_attrs.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.
		season (str, optional): Used to define different seasons. 2010 is the earliest available season.
			Most recent season is the default.
		metric (str, optional): Used to get highest ranking games for different metrics. Available values are:
			'Excitement', 'Tension', 'Dominance', 'ComeBack', 'FanMatch', 'Upsets', and 'Busts'. Default is
			'Excitement'. 'FanMatch', 'Upsets', and 'Busts' are only valid for seasons after 2010.

	Returns:
		ga_df (pandas dataframe): Pandas dataframe containing the Game Attributes table from kenpom.com for a
		given metric.

	Raises:
		ValueError: If `season` is less than 2010.
		KeyError: If `metric` is invalid.
	"""

	# `metric` parameter checking.
	metric = metric.upper()
	metrics = {'EXCITEMENT': 'Excitement', 'TENSION': 'Tension', 'DOMINANCE': 'Dominance', 'COMEBACK': 'MinWP',
				'FANMATCH': 'FanMatch', 'UPSETS': 'Upsets', 'BUSTS': 'Busts'}
	if metric not in metrics:
		raise KeyError(
			"""Metric is invalid, must be one of: 'Excitement',
				'Tension', 'Dominance', 'ComeBack', 'FanMatch', 'Upsets', and 'Busts'""")
	else:
		met_url = 's=' + metrics[metric]

	url = 'https://kenpom.com/game_attrs.php?' + met_url

	# Season selection and an additional check.
	if season:
		if int(season) < 2010:
			raise ValueError(
				'Season cannot be less than 2010, as data only goes back that far.')
		elif int(season) < 2011 and metric.upper() in ['FANMATCH', 'UPSETS', 'BUSTS']:
			raise ValueError(
				'FanMatch, Upsets, and Busts tables only available for seasons after 2010.'
			)
		url = url + '&y=' + str(season)

	browser.open(url)
	playerstats = browser.get_current_page()

	table = playerstats.find_all('table')[0]
	ga_df = pd.read_html(str(table))

	# Dataframe tidying.
	ga_df = ga_df[0]
	ga_df.columns = ['Rank', 'Date', 'Game', 'Box', 'Location', 'Conf.Matchup', 'Value']
	ga_df = ga_df.drop(['Box'], axis=1)
	ga_df['Location'], ga_df['Arena'] = ga_df['Location'].str.split(' \(').str
	ga_df['Arena'] = ga_df['Arena'].str.rstrip(')')

	return ga_df


def get_program_ratings(browser):
	"""
	Scrapes the program ratings table (https://kenpom.com/programs.php) into a dataframe.

	Args:
		browser (mechanicalsoup StatefulBrowser): Authenticated browser with full access to kenpom.com generated
			by the `login` function.

	Returns:
		programs_df (pandas dataframe): Pandas dataframe containing the program ratings table from kenpom.com.
	"""

	url = 'https://kenpom.com/programs.php'

	browser.open(url)
	programs = browser.get_current_page()
	table = programs.find_all('table')[0]
	programs_df = pd.read_html(str(table))
	programs_df = programs_df[0]

	programs_df.columns = ['Rank', 'Team', 'Rating', 'kenpom.Best.Rank', 'kenpom.Best.Season', 'kenpom.Worst.Rank',
							'kenpom.Worst.Season', 'kenpom.Median.Rank', 'kenpom.Top10.Finishes',
							'kenpom.Top25.Finishes', 'kenpom.Top50.Finishes', 'NCAA.Champs', 'NCAA.F4', 'NCAA.E8',
							'NCAA.S16', 'NCAA.R1']

	programs_df = programs_df[programs_df.Team != 'Team']

	return programs_df

def df_name_clean(dataframe):
	for index in dataframe.index:
		iter_team = dataframe.loc[index,'Team']
		iter_conf = dataframe.loc[index,'Conference']

		try:
			if iter_team[-3:] == "St.":
				dataframe.loc[index,'Team'] = dataframe.loc[index,'Team'][:-1] + "ate"
				
			if iter_team == 'Miami OH':	
				dataframe.loc[index,'Team'] = 'Miami (OH)'
			if iter_team == "Miami FL":	
				dataframe.loc[index,'Team'] = "Miami (FL)"
			if iter_team == "St. Francis PA":	
				dataframe.loc[index,'Team'] = "St. Francis (PA)"
			if iter_team == "Bryant":	
				dataframe.loc[index,'Team'] = "Bryant University"
			if iter_team == "Southern":	
				dataframe.loc[index,'Team'] = "Southern University"
			if iter_team == "Bethune Cookman":	
				dataframe.loc[index,'Team'] = "Bethune-Cookman"
			if iter_team == "Cal Baptist":	
				dataframe.loc[index,'Team'] = "California Baptist"	
			if iter_team == "The Citadel":
				dataframe.loc[index,'Team'] = "Citadel"	
			if iter_team == "Central Connecticut":
				dataframe.loc[index,'Team'] = "Central Connecticut State"
			if iter_team == "Loyola MD":
				dataframe.loc[index,'Team'] = "Loyola (MD)"
			if iter_team == "Cal St. Bakersfield":	
				dataframe.loc[index,'Team'] = "Cal State Bakersfield"
			if iter_team == "LIU":	
				dataframe.loc[index,'Team'] = "LIU Brooklyn"
			if iter_team == "NJIT":	
				dataframe.loc[index,'Team'] = "N.J.I.T."
			if iter_team == "Louisiana Monroe":	
				dataframe.loc[index,'Team'] = "Louisiana-Monroe"
			if iter_team == "Illinois Chicago":	
				dataframe.loc[index,'Team'] = "Illinois-Chicago"
			if iter_team == "Gardner Webb":	
				dataframe.loc[index,'Team'] = "Gardner-Webb"
			if iter_team == "Nicholls St.":	
				dataframe.loc[index,'Team'] = "Nicholls State"
			if iter_team == "Cal St. Northridge":	
				dataframe.loc[index,'Team'] = "Cal State Northridge"
			if iter_team == "UNC Wilmington":	
				dataframe.loc[index,'Team'] = "North Carolina-Wilmington"
			if iter_team == 'Mississippi':	
				dataframe.loc[index,'Team'] = 'Ole Miss'
			if iter_team == 'Tennessee Martin':	
				dataframe.loc[index,'Team'] = 'Tennessee-Martin'
			if iter_team == 'SIU Edwardsville':	
				dataframe.loc[index,'Team'] = 'SIU-Edwardsville'
			if iter_team == 'Montana St.':	
				dataframe.loc[index,'Team'] = 'Montana State'
			if iter_team == 'Nebraska Omaha':	
				dataframe.loc[index,'Team'] = 'Nebraska-Omaha'
			if iter_team == "Long Beach":
				dataframe.loc[index,'Team'] = "Long Beach State"	

			# Changes unique dataframe values of kenpom.com/index 
			if iter_team == "Saint Mary" and iter_conf == "WCC":
				dataframe.loc[index,'Team'] = "Saint Mary's"
			if iter_team == "North Carolina Central " and iter_conf == "MEAC":
				dataframe.loc[index,'Team'] = "North Carolina Central"
			if iter_team == "Boston University " and iter_conf == "Pat":
				dataframe.loc[index,'Team'] = "Boston University"
			if iter_team == "Texas A&M Corpus Chris" and iter_conf == "Slnd":
				dataframe.loc[index,'Team'] = "Texas A&M-CC"
			if iter_team == "Maryland Eastern Shore" and iter_conf == "MEAC":
				dataframe.loc[index,'Team'] = "Maryland-Eastern Shore"
			if iter_team == "St. Francis NY" and iter_conf == "NEC": 	
				dataframe.loc[index,'Team'] = "St. Francis (BKN)"
			if iter_team == "Saint Joseph's" and iter_conf == "A10": 	
				dataframe.loc[index,'Team'] = "Saint Joseph's (PA)"
			if iter_team == "Saint Peter's" and iter_conf == "MAAC": 	
				dataframe.loc[index,'Team'] = "St. Peter's"
			if iter_team == "Cal St. Fullerton" and iter_conf == "BW": 	
				dataframe.loc[index,'Team'] = "Cal State Fullerton"
			if iter_team == "Arkansas Pine Bluff" and iter_conf == "SWAC":
				dataframe.loc[index,'Team'] = "Arkansas-Pine Bluff"
			if iter_team == "College of Charleston" and iter_conf == "CAA": # for years < 2019
				dataframe.loc[index,'Team'] = "Charleston"

			if 	iter_team == "Fort Wayne" or iter_team == "IPFW":
				dataframe.loc[index,'Team'] = "Purdue Fort Wayne"	
			if 	iter_team == "Arkansas Little Rock":
				dataframe.loc[index,'Team'] = "Little Rock"		
			if 	iter_team == "Texas Pan American":
				dataframe.loc[index,'Team'] = "UT Rio Grande Valley"
			if iter_team == "Louisiana Lafayette":
				dataframe.loc[index,'Team'] = "Louisiana"
			if iter_team == "College of Charleston":
				dataframe.loc[index,'Team'] = "Charleston"
			if iter_team == "NJIT": 
				dataframe.loc[index,'Team'] = "N.J.I.T."

		except:
			pass
		pass
	return dataframe

	return

def get_team_df(browser, season=None):

	url = 'https://kenpom.com/summary.php'

	if season:
		if int(season) < 2002:
			raise ValueError(
				'season cannot be less than 2002, as data only goes back that far.')
		url = url + '?y=' + str(season)

	browser.open(url)

	tag_string = "kenpom.com/summary"

	browser.open(url)
	eff = browser.get_current_page()
	table = eff.find_all('table')[0]
	eff_df = pd.read_html(str(table))

	# Dataframe tidying.
	eff_df = eff_df[0]
	if len(eff_df.columns) == 18:
		eff_df = eff_df.iloc[:, 0:18]
		eff_df.columns = ['Team', 'Conference', 'Tempo-Adj', 'Tempo-Adj.Rank', 'Tempo-Raw', 'Tempo-Raw.Rank',
						  'Avg. Poss Length-Offense', 'Avg. Poss Length-Offense.Rank', 'Avg. Poss Length-Defense',
						  'Avg. Poss Length-Defense.Rank', 'Off. Efficiency-Adj', 'Off. Efficiency-Adj.Rank',
						  'Off. Efficiency-Raw', 'Off. Efficiency-Raw.Rank', 'Def. Efficiency-Adj',
						  'Def. Efficiency-Adj.Rank', 'Def. Efficiency-Raw', 'Def. Efficiency-Raw.Rank']
	else:
		eff_df = eff_df.iloc[:, 0:14]
		eff_df.columns = ['Team', 'Conference', 'Tempo-Adj', 'Tempo-Adj.Rank', 'Tempo-Raw', 'Tempo-Raw.Rank',
						  'Off. Efficiency-Adj', 'Off. Efficiency-Adj.Rank', 'Off. Efficiency-Raw',
						  'Off. Efficiency-Raw.Rank', 'Def. Efficiency-Adj', 'Def. Efficiency-Adj.Rank',
						  'Def. Efficiency-Raw', 'Def. Efficiency-Raw.Rank']

	# Remove the header rows that are interjected for readability.
	eff_df = eff_df[eff_df.Team != 'Team']

	tmp_df_names = eff_df.iloc[:,0].str.replace('\d+', '').str.rstrip() # these don't have astricks? 
	tmp_df_ranks =  eff_df.iloc[:,0].str.extract(r"(\d+)") 
	tmp_df_names = tmp_df_names.rename("Team")

	test_list = tmp_df_names.tolist()

	eff_df["Team"] = tmp_df_names

	df = eff_df[["Team", "Conference"]].copy()

	df["Season"] = season
	
	#print(df)

	df = df_name_clean(dataframe=df)
	#print(df)
	return df #test_list

# def name_clean_up(dataframe, year=None):
# 	for index in dataframe.index:
# 		iter_team = dataframe.loc[index,'Team']
# 		iter_conf = dataframe.loc[index,'Conf']

# 		try:
# 			if iter_team[-3:] == "St.":
# 				dataframe.loc[index,'Team'] = dataframe.loc[index,'Team'][:-1] + "ate"
				
# 			if iter_team == 'Miami OH':	
# 				dataframe.loc[index,'Team'] = 'Miami (OH)'
# 			if iter_team == "Miami FL":	
# 				dataframe.loc[index,'Team'] = "Miami (FL)"
# 			if iter_team == "St. Francis PA":	
# 				dataframe.loc[index,'Team'] = "St. Francis (PA)"
# 			if iter_team == "Bryant":	
# 				dataframe.loc[index,'Team'] = "Bryant University"
# 			if iter_team == "Southern":	
# 				dataframe.loc[index,'Team'] = "Southern University"
# 			if iter_team == "Bethune Cookman":	
# 				dataframe.loc[index,'Team'] = "Bethune-Cookman"
# 			if iter_team == "Cal Baptist":	
# 				dataframe.loc[index,'Team'] = "California Baptist"	
# 			if iter_team == "The Citadel":
# 				dataframe.loc[index,'Team'] = "Citadel"	
# 			if iter_team == "Central Connecticut":
# 				dataframe.loc[index,'Team'] = "Central Connecticut State"
# 			if iter_team == "Loyola MD":
# 				dataframe.loc[index,'Team'] = "Loyola (MD)"
# 			if iter_team == "Cal St. Bakersfield":	
# 				dataframe.loc[index,'Team'] = "Cal State Bakersfield"
# 			if iter_team == "LIU":	
# 				dataframe.loc[index,'Team'] = "LIU Brooklyn"
# 			if iter_team == "NJIT":	
# 				dataframe.loc[index,'Team'] = "N.J.I.T."
# 			if iter_team == "Louisiana Monroe":	
# 				dataframe.loc[index,'Team'] = "Louisiana-Monroe"
# 			if iter_team == "Illinois Chicago":	
# 				dataframe.loc[index,'Team'] = "Illinois-Chicago"
# 			if iter_team == "Gardner Webb":	
# 				dataframe.loc[index,'Team'] = "Gardner-Webb"
# 			if iter_team == "Nicholls St.":	
# 				dataframe.loc[index,'Team'] = "Nicholls State"
# 			if iter_team == "Cal St. Northridge":	
# 				dataframe.loc[index,'Team'] = "Cal State Northridge"
# 			if iter_team == "UNC Wilmington":	
# 				dataframe.loc[index,'Team'] = "North Carolina-Wilmington"
# 			if iter_team == 'Mississippi':	
# 				dataframe.loc[index,'Team'] = 'Ole Miss'
# 			if iter_team == 'Tennessee Martin':	
# 				dataframe.loc[index,'Team'] = 'Tennessee-Martin'
# 			if iter_team == 'SIU Edwardsville':	
# 				dataframe.loc[index,'Team'] = 'SIU-Edwardsville'
# 			if iter_team == 'Montana St.':	
# 				dataframe.loc[index,'Team'] = 'Montana State'
# 			if iter_team == 'Nebraska Omaha':	
# 				dataframe.loc[index,'Team'] = 'Nebraska-Omaha'
# 			if iter_team == "Long Beach":
# 				dataframe.loc[index,'Team'] = "Long Beach State"	

# 			# Changes unique dataframe values of kenpom.com/index 
# 			if iter_team == "Saint Mary" and iter_conf == "WCC":
# 				dataframe.loc[index,'Team'] = "Saint Mary's"
# 			if iter_team == "North Carolina Central " and iter_conf == "MEAC":
# 				dataframe.loc[index,'Team'] = "North Carolina Central"
# 			if iter_team == "Boston University " and iter_conf == "Pat":
# 				dataframe.loc[index,'Team'] = "Boston University"
# 			if iter_team == "Texas A&M Corpus Chris" and iter_conf == "Slnd":
# 				dataframe.loc[index,'Team'] = "Texas A&M-CC"
# 			if iter_team == "Maryland Eastern Shore" and iter_conf == "MEAC":
# 				dataframe.loc[index,'Team'] = "Maryland-Eastern Shore"
# 			if iter_team == "St. Francis NY" and iter_conf == "NEC": 	
# 				dataframe.loc[index,'Team'] = "St. Francis (BKN)"
# 			if iter_team == "Saint Joseph's" and iter_conf == "A10": 	
# 				dataframe.loc[index,'Team'] = "Saint Joseph's (PA)"
# 			if iter_team == "Saint Peter's" and iter_conf == "MAAC": 	
# 				dataframe.loc[index,'Team'] = "St. Peter's"
# 			if iter_team == "Cal St. Fullerton" and iter_conf == "BW": 	
# 				dataframe.loc[index,'Team'] = "Cal State Fullerton"
# 			if iter_team == "Arkansas Pine Bluff" and iter_conf == "SWAC":
# 				dataframe.loc[index,'Team'] = "Arkansas-Pine Bluff"
# 			if iter_team == "College of Charleston":
# 				dataframe.loc[index,'Team'] = "Charleston"

# 		except:
# 			pass
# 		pass
# 	return dataframe
