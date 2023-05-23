import json
import logging
import os
import sys
import networkx as nx
import math
import pandas

from collections import Counter
from glob import glob

from dcnet.util import calc_homogeneity
from dcnet.util import dumpJsonToFile
from dcnet.util import genericErrorInfo
from dcnet.util import get_mov_imdb_id
from dcnet.util import getDictFromFile
from dcnet.util import getDictFromJsonGZ
from dcnet.util import gzipTextFile

from dcnet.imdb_scraper import get_full_credits_for_director
from dcnet.imdb_scraper import get_full_crew_for_movie
from dcnet.imdb_scraper import is_feature_film
from dcnet.imdb_scraper import is_feature_film_v2

from NwalaTextUtils.textutils import parallelTask

logger = logging.getLogger('dcnet.dcnet')

'''
    G.nodes[n]['name'] = "Wes Anderson"
'''

def get_director_metadata(director_metadata_file):

    movie_dir_details = {}
    if( director_metadata_file != '' ):
        try:
            director_metadata_file = pandas.read_csv(director_metadata_file, keep_default_na=False)
            for index, row in director_metadata_file.iterrows():
                
                dir_id = get_mov_imdb_id(row['IMDb_URI'], split_key='/name/')

                movie_dir_details[dir_id] = {k.lower(): v for k, v in row.to_dict().items()}
                movie_dir_details[dir_id]['director_id'] = dir_id
        except:
            genericErrorInfo()

    return movie_dir_details

def write_director_movie_credits(director_id, repo, max_movies, cache_read=True, **kwargs):

    logger.info('\nwrite_director_movie_credits()')
    logger.info(f'\tcache_read: {cache_read}')
    
    total_directors = len(director_id)
    director_metadata = get_director_metadata(kwargs.get('director_metadata_file', ''))
    
    if( repo is not None ):
        os.makedirs(repo, exist_ok=True)

    for i in range(total_directors):
        
        #write director credits - start
        dir_cred = {}
        dir_id = director_id[i]
        dir_cred_filepath = f'{repo}{dir_id}'
        dir_cred_file = f'{dir_cred_filepath}/credits.json'

        os.makedirs(dir_cred_filepath, exist_ok=True)
        os.makedirs(f'{dir_cred_filepath}/movies/', exist_ok=True)
        if( cache_read is True ):
            dir_cred = getDictFromFile(dir_cred_file)
        
        if( len(dir_cred) == 0 ):
            dir_cred = get_full_credits_for_director(dir_id)
            if( len(dir_cred) != 0 ):
                dir_cred['details'] = director_metadata.get(dir_id, {})
                dumpJsonToFile(dir_cred_file, dir_cred, indentFlag=False)

        credits = dir_cred.get('credits', [])
        director_name = dir_cred.get('director_name', '')
        total_movies = len(credits)
        logger.info(f'\n\tdirector {i+1} of {total_directors}, {director_name}, {total_movies} movies\n')
        #write director credits - end


        if( max_movies is not None and max_movies > -1 ):
            credits = credits[:max_movies]
            total_movies = len(credits)

        movie_crew_jobs = []
        for j in range(total_movies):
            
            mov = credits[j]
            mov_imdb_uri = mov.get('uri', '')
            mov_imdb_id = get_mov_imdb_id(mov_imdb_uri)
            keywords = {'title_id': mov_imdb_id, 'set_imdb_details': True}
            print_msg = f'\t\tmov {j+1} of {total_movies}'
            mov_file_path = f'{dir_cred_filepath}/movies/{mov_imdb_id}.json.gz'

            if( cache_read is True and os.path.exists(mov_file_path) is True ):
                logger.info(f'\t\tmovie cache hit, would skip writing: {mov_file_path}')
                continue
            movie_crew_jobs.append({'func': get_full_crew_for_movie, 'args': keywords, 'misc': {'mov_file_path': mov_file_path, 'director_id': dir_id}, 'print': print_msg})

        res_lst = parallelTask(movie_crew_jobs)
        total_movies = len(res_lst)
        for j in range(total_movies):
            
            mov = res_lst[j]
            if( len(mov['output']) == 0 ):
                continue
            
            mov_imdb_id = mov['input']['args']['title_id']
            mov_file_path = mov['misc']['mov_file_path']
            movie_title = mov['output'].get('title', '')
            mov['output']['director_id'] = mov['misc']['director_id']

            gzipTextFile(mov_file_path, json.dumps(mov['output'], ensure_ascii=False))
            logger.info(f'\t\twrote crew info {j+1} of {total_movies}: {movie_title}')

def normalize_movie_role(role):
    
    '''
        Notes
        1. Don't create distinction between series and movie roles, therefore remove "Series prefix"
        2. Nomalize all variant of "Cast" and "Writing Credit" to their root roles
    '''

    '''
        Oscar categories
        https://awardsdatabase.oscars.org/
        ACTING
        ANIMATED FEATURE FILM
        CINEMATOGRAPHY
        COSTUME DESIGN
        DIRECTING
        DOCUMENTARY
        FILM EDITING
        INTERNATIONAL FEATURE FILM
        MAKEUP
        MUSIC - SCORING
        MUSIC - SONG
        PICTURE
        PRODUCTION DESIGN
        SCIENTIFIC AND TECHNICAL
        SHORT FILMS - ANIMATED
        SHORT FILMS - LIVE ACTION
        SOUND
        SPECIAL EFFECTS
        VISUAL EFFECTS
        WRITING
    '''

    '''
        1. Additional Crew 1666
        2. Animation Department 405
        3. Art Department 1427
        4. Art Direction by 1226
        5. Camera and Electrical Department 1661
        6. Cast 739
        7. Cast (in credits order) 506
        8. Cast (in credits order) complete, awaiting verification 332
        9. Cast (in credits order) verified as complete 795
        10. Cast complete, awaiting verification 9
        11. Casting By 1241
        12. Casting Department 1200
        13. Cinematography by 1866
        14. Costume Design by 1247
        15. Costume and Wardrobe Department 1282
        16. Directed by 2573
        17. Editorial Department 1509
        18. Film Editing by 1832
        19. Location Management 1209
        20. Makeup Department 1399
        21. Music Department 1436
        22. Music by 1652
        23. Produced by 2078
        24. Production Department 3
        25. Production Design by 1358
        26. Production Management 1462
        27. Script and Continuity Department 1215
        28. Second Unit Director or Assistant Director 1446
        29. Series Additional Crew 386
        30. Series Animation Department 127
        31. Series Art Department 362
        32. Series Art Direction by 326
        33. Series Camera and Electrical Department 383
        34. Series Cast 376
        35. Series Cast complete, awaiting verification 21
        36. Series Cast verified as complete 9
        37. Series Casting By 333
        38. Series Casting Department 331
        39. Series Cinematography by 372
        40. Series Costume Design by 339
        41. Series Costume and Wardrobe Department 345
        42. Series Directed by 419
        43. Series Editorial Department 371
        44. Series Film Editing by 381
        45. Series Location Management 327
        46. Series Makeup Department 358
        47. Series Music Department 367
        48. Series Music by 350
        49. Series Produced by 415
        50. Series Production Design by 347
        51. Series Production Management 367
        52. Series Script and Continuity Department 334
        53. Series Second Unit Director or Assistant Director 360
        54. Series Set Decoration by 328
        55. Series Sound Department 383
        56. Series Special Effects by 289
        57. Series Stunts 329
        58. Series Thanks 136
        59. Series Transportation Department 320
        60. Series Visual Effects by 334
        61. Series Writing Credits 406
        62. Series Writing Credits (WGA) 1
        63. Set Decoration by 1121
        64. Sound Department 1644
        65. Special Effects by 1096
        66. Stunts 1121
        67. Thanks 1020
        68. Transportation Department 1154
        69. Visual Effects by 1184
        70. Writing Credits 692
        71. Writing Credits (WGA) 541
        72. Writing Credits (WGA) (in alphabetical order) 6
        73. Writing Credits (in alphabetical order) 639
    '''


    if( role.startswith('Series ') ):
        role = role[7:]

    for pre in ['Cast ', 'Writing Credits ']:
        if( role.startswith(pre) ):
            role = pre[:-1]

    return role

def get_movie_crew(all_crew_details, movie_id, director_id, full_credits):

    for c in full_credits:
        
        if( c['role'] == 'Directed by' ):
            continue
        
        for memb in c['crew']:
            crew_imdb_id = get_mov_imdb_id( memb['link'], split_key='/name/' )
            '''
                Notes
                * A single crew member could have multiple roles in a single movie, so collect all possible roles in list. 
            '''
            all_crew_details.setdefault( crew_imdb_id, {'name': memb['name'], 'roles': {}} )
            all_crew_details[crew_imdb_id]['roles'].setdefault( f'{director_id}_{movie_id}', set() )
            all_crew_details[crew_imdb_id]['roles'][ f'{director_id}_{movie_id}' ].add( c['role'] )

def add_movie_crew_stat(all_crew_details):

    for crew_imdb_id, crew_dets in all_crew_details.items():
        
        directors = set()
        movies = set()
        crew_roles = set()

        for dir_crew in crew_dets['roles'].keys():
            crew_roles = crew_roles | crew_dets['roles'][dir_crew]
            crew_dets['roles'][dir_crew] = list(crew_dets['roles'][dir_crew])

            dir_crew = dir_crew.split('_')
            directors.add( dir_crew[0] )
            movies.add( dir_crew[1] )

        crew_dets['unique_director'] = len(directors)
        crew_dets['unique_movie'] = len(movies)
        crew_dets['unique_role'] = len(crew_roles)


def traverse_movies_for_details(repo, exclude_movie_types, **kwargs):
    
    def normalize_all_movie_roles(mov, exclude_movie_roles):
        
        new_full_credit = []
        
        for i in range( len(mov['full_credits']) ):
            
            mov['full_credits'][i]['role'] = normalize_movie_role(mov['full_credits'][i]['role'])
            if( mov['full_credits'][i]['role'] in exclude_movie_roles ):
                continue
            new_full_credit.append( mov['full_credits'][i] )

        mov['full_credits'] = new_full_credit

    director_metadata = get_director_metadata(kwargs.get('director_metadata_file', ''))
    exclude_movie_roles = kwargs.get('exclude_movie_roles', [])

    print('\ntraverse_movies_for_details()')
    print('\texclude_movie_types:', exclude_movie_types)
    print(f'\trepo: {repo}')
    
    roles = []
    director_ids = []
    all_crew_details = {}
    generic_mov_stats = {'feature_films': 0, 'movie_types': []}
    exclude_feature_film = True if 'feature_films' in exclude_movie_types else False
    exclude_non_feature_film = True if 'non_feature_films' in exclude_movie_types else False

    for mov in glob(f'{repo}/*/movies/*.json.gz'):
        
        mov = getDictFromJsonGZ(mov)
        if( len(mov) == 0 ):
            continue
        
        movie_type = mov['imdb_details'].get('type', '')
        if( movie_type in exclude_movie_types ):
            continue
        movie_id = get_mov_imdb_id(mov['title_uri'])
        
        is_film_feature = is_feature_film('', movie=mov['imdb_details'])
        #is_film_feature = is_feature_film_v2(movie_id)
        if( exclude_feature_film and is_film_feature ):
            continue

        if( exclude_non_feature_film and is_film_feature is False ):
            continue
        
        normalize_all_movie_roles(mov, exclude_movie_roles)
        get_movie_crew( all_crew_details, movie_id, mov['director_id'], mov['full_credits'] )

        director_ids.append(mov['director_id'])
        roles += [r['role'] for r in mov['full_credits']]
        generic_mov_stats['movie_types'].append(movie_type)

        if( is_film_feature is True ):
            generic_mov_stats['feature_films'] += 1

    roles = Counter(roles)
    director_ids = Counter(director_ids)
    add_movie_crew_stat(all_crew_details)
    generic_mov_stats['movie_types'] = Counter( generic_mov_stats['movie_types'] )

    for dir_id in director_metadata:
        director_metadata[dir_id]['total_movies_directed'] = director_ids.get(dir_id, -1)

    return {
        'roles': roles,
        'director_ids': director_ids,
        'all_crew_details': all_crew_details,
        'generic_mov_stats': generic_mov_stats,
        'director_metadata': director_metadata
    }

def print_stats(repo, exclude_movie_types, **kwargs):

    res = traverse_movies_for_details(repo, exclude_movie_types, **kwargs)
    roles = res['roles']
    generic_mov_stats = res['generic_mov_stats']

    total_movies = sum(res['director_ids'].values())
    total_directors = len(res['director_ids'])
    
    print('\nSummary\nTotal movies: {:,}'.format(total_movies))
    print('Total feature films: {:,}'.format(generic_mov_stats['feature_films']))
    print('Movie types:', generic_mov_stats['movie_types'])
    print(f'Total # directors: {total_directors}')
    print('Avg. # movies per directors: {:.2f}'.format(total_movies/total_directors))

    print( '\nTotal crews: {:,}'.format(len(res['all_crew_details'])) )

    print('\nTotal roles:', len(res['roles']))
    roles = sorted( roles.items(), key=lambda x: x[0])
    for i in range(len(roles)):
        print( '\t{}. {} {}'.format(i+1, roles[i][0], roles[i][1]) )


def gen_movie_crew_graph(all_crew_details, add_self_loops=False):

    director_crew_graph = nx.Graph()
    self_loops = 0
    for crew_id, crew_dets in all_crew_details.items():

        director_ids = []
        for dir_crew in crew_dets['roles'].keys():
            director_id, movie_id = dir_crew.split('_')
            director_ids.append(director_id)

        director_ids = Counter(director_ids)
        #total_dir_count: total count of directors a specific crew has worked with
        total_dir_count = sum(director_ids.values())
        for dir_id, cofeat_count in director_ids.items():
            
            '''
                Notes
                * A director could also be a crew member in the directed movie, so avoid self-loops by skipping them
            '''
            if( add_self_loops is False and dir_id == crew_id ):
                self_loops += 1
                #e.g., nm0000881.json
                continue
            
            #cofeat_rate is the fraction of times a crew has worked with the director irrespective of the role
            cofeat_rate = cofeat_count/total_dir_count
            director_crew_graph.add_edge(dir_id, crew_id, cofeat_rate=cofeat_rate)
            
    return director_crew_graph

def add_attributes_to_mov_crew_graph(all_crew_details, director_crew_graph, director_metadata):
    
    def add_employee_dist(crew_employee_id, crew_employee_dist, roles):
        for r in roles:
            crew_employee_dist.setdefault(r, {})
            crew_employee_dist[r].setdefault(crew_employee_id, 0)
            crew_employee_dist[r][crew_employee_id] += 1

    for crew_id, crew_dets in all_crew_details.items():
        director_crew_graph.nodes[crew_id]['node_type'] = 'crew'
        director_crew_graph.nodes[crew_id]['name'] = crew_dets['name']
        director_crew_graph.nodes[crew_id]['cust_size'] = 1

    
    for dir_id, dir_dets in director_metadata.items():
        
        #print('director_id:', dir_id, 'employees:', director_crew_graph.degree(dir_id), 'movies directed:', dir_dets['total_movies_directed'])
        #print('director_id:', dir_id)

        avg_crew_homogeneity = 0
        #dir_dets['crew_employee_dist']: key is role, value is list of IMDb Ids of folks that have functioned in that role
        dir_dets['crew_employee_dist'] = {}
        dir_dets['avg_role_homogeneity'] = {'sum_role_homogeneity': 0}
        
        for crew_employee_id in director_crew_graph.neighbors(dir_id):

            '''
                Note:
                Why crew nm0000881 threw key error for dir_id: nm0009190.
                    for dir_crew, role_dets in all_crew_details[crew_employee_id]['roles'].items():
                I suspect this happened because nm0000881 is a director and had a role that excluded it from being added to all_crew_details. Thus when nm0009190 invoked it's neighbors, nm0000881 is a neighbor but one without an entry in all_crew_details
            '''
            for dir_crew, role_dets in all_crew_details.get(crew_employee_id, {'roles': {}})['roles'].items():
                
                #dir_crew is a unique key of format directorid_movieid
                if( dir_crew.startswith(dir_id) is False ):
                    continue
                
                #every dir_crew here corresponds to a unique movie in which dir_id and crew_employee_id co-costarred
                add_employee_dist(crew_employee_id, dir_dets['crew_employee_dist'], role_dets)
                #print(f'\t\t{dir_crew}', role_dets, crew_employee_id)
            #print()

        for role, employee_dist in dir_dets['crew_employee_dist'].items():
            
            #employee_dist = Counter(dir_dets['crew_employee_dist'][role])
            
            #dir_dets['crew_employee_dist'][role]: key is the crew_id, value is the number of times they've worked with dir_id
            total_roles_director_employed = sum(employee_dist.values())
            
            for crew_id, crew_co_feat in employee_dist.items():
                
                weight = crew_co_feat/total_roles_director_employed

                #note (see also remedy for bi-links): it's possible for the same crew to work with the same director under different roles, capture the weights for the different roles, and ensure edges account for these
                director_crew_graph[dir_id][crew_id].setdefault('weights', [])
                director_crew_graph[dir_id][crew_id]['weights'].append(weight)
                
                director_crew_graph[dir_id][crew_id].setdefault('roles', [])
                director_crew_graph[dir_id][crew_id]['roles'].append(role) 
                

            unique_count = len(employee_dist.keys())
            role_homogeneity = calc_homogeneity( unique_count, total_roles_director_employed )
            dir_dets['crew_employee_dist'][role] = {'employee_dist': employee_dist, 'role_homogeneity': role_homogeneity}
            dir_dets['avg_role_homogeneity']['sum_role_homogeneity'] += role_homogeneity

        dir_dets['avg_role_homogeneity'] = dir_dets['avg_role_homogeneity']['sum_role_homogeneity']/len(dir_dets['crew_employee_dist'])
       
        #director_crew_graph.nodes[dir_id]['node_type'] = 'director'
        director_crew_graph.nodes[dir_id]['name'] = dir_dets['firstname'] + ' ' + dir_dets['lastname']
        director_crew_graph.nodes[dir_id]['node_type'] = '{}{}{}'.format(dir_dets['sex'], dir_dets['ethnicity_race'], dir_dets['labels'])

        director_crew_graph.nodes[dir_id]['avg_role_homogeneity'] = dir_dets['avg_role_homogeneity']
        director_crew_graph.nodes[dir_id]['cust_size'] = 1000 * director_crew_graph.nodes[dir_id]['avg_role_homogeneity']

    
    #remedy for bi-links:
    for dir_id, crew_id in director_crew_graph.edges:
        
        if( 'weights' not in director_crew_graph[dir_id][crew_id] ):
            continue
            
        if( len(director_crew_graph[dir_id][crew_id]['weights']) == 1 ):
            director_crew_graph[dir_id][crew_id]['weight'] = director_crew_graph[dir_id][crew_id].pop('weights')[0]
            director_crew_graph[dir_id][crew_id]['role'] = director_crew_graph[dir_id][crew_id].pop('roles')[0]
        else:
            max_weight = max( director_crew_graph[dir_id][crew_id]['weights'] )
            director_crew_graph[dir_id][crew_id]['weight'] = max_weight

            arg_max_weight = director_crew_graph[dir_id][crew_id].pop('weights').index(max_weight)
            director_crew_graph[dir_id][crew_id]['role'] = director_crew_graph[dir_id][crew_id]['roles'][arg_max_weight]
            director_crew_graph[dir_id][crew_id].pop('roles')
            

def gen_movie_crew_net(repo, exclude_movie_types, **kwargs):

    def print_dir_role_homogeneity_dets(director_metadata):

        logger.info( '\ndirector avg. role homogeneity (ARH)' )
        logger.info( '#, labels, ARH, firstname, lastname, top 3 homogeneous roles' )

        all_dir_dets = sorted( director_metadata.items(), key=lambda x: x[1]['avg_role_homogeneity'], reverse=True )
        
        for i in range(len(all_dir_dets)):

            #dict_keys(['lastname', 'firstname', 'sex', 'ethnicity_race', 'labels', 'imdb_uri', 'director_id', 'total_movies_directed', 'crew_employee_dist', 'avg_crew_homogeneity'])
            dir_id = all_dir_dets[i][0]
            dir_dets = all_dir_dets[i][1]
            
            crew_homogeneity = sorted( dir_dets['crew_employee_dist'].items(), key=lambda x: x[1]['role_homogeneity'], reverse=True )[:3]
            crew_homogeneity = ' & '.join([c[0] for c in crew_homogeneity])

            firstname = dir_dets['firstname']
            lastname = dir_dets['lastname']
            avg_crew_homogeneity = dir_dets['avg_role_homogeneity']
            dir_lab = '{}{}{}'.format(dir_dets['sex'], dir_dets['ethnicity_race'], dir_dets['labels'])
            
            logger.info( '{:3d}. {:<3} {:.3f} {:<25} {}'.format(i+1, dir_lab, avg_crew_homogeneity, firstname + ' ' + lastname, crew_homogeneity) )

    logger.info('\ngen_movie_crew_net():')
    add_self_loops = kwargs.get('self_loops', False)

    res = traverse_movies_for_details(repo, exclude_movie_types, **kwargs)
    all_crew_details = res['all_crew_details']
    director_metadata = res['director_metadata']

    
    director_crew_graph = gen_movie_crew_graph(all_crew_details, add_self_loops=add_self_loops)
    add_attributes_to_mov_crew_graph(all_crew_details, director_crew_graph, director_metadata)
    print_dir_role_homogeneity_dets(director_metadata)

    '''
    dumpJsonToFile('crew_nm0027572.json', all_crew_details['nm0027572'])
    dumpJsonToFile('crew_nm0000229.json', all_crew_details['nm0000229'])
    dumpJsonToFile('dir_nm0027572.json', director_metadata['nm0027572'])
    dumpJsonToFile('dir_nm0000229.json', director_metadata['nm0000229'])
    '''

    logger.info('\tis connected: {}, writing director_crew_graph.gexf'.format(nx.is_connected(director_crew_graph)))
    nx.write_gexf(director_crew_graph, 'director_crew_graph.gexf')
    logger.info('\tdone writing')
