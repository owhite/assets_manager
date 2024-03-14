# {'Short_name': {'table': 'project', 'field': 'short_name', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Title': {'table': 'project', 'field': 'title', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Description': {'table': 'project', 'field': 'description', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Program': {'table': 'program', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, 
# {'Knowledgebase URL': {'table': 'project', 'field': 'url_knowledgebase', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Comment': {'table': 'project', 'field': 'comment', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Project type': {'table': None, 'field': None, 'optional': True, 'searchable': False, 'list': ['grant', 'study']}}, 
# {'Lab name': {'table': 'lab', 'field': 'lab_name', 'optional': True, 'searchable': True, 'list': None}}, 
# {'Contributors': {'table': 'contributor', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, 
# {'Is grant?': {'table': None, 'field': None, 'optional': True, 'searchable': False, 'list': ['yes', 'no']}}, 
# {'Grant number?': {'table': 'grant_info', 'field': 'grant_number', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Funding agency': {'table': 'grant_info', 'field': 'funding_agency', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Description URL': {'table': 'grant_info', 'field': 'description_url', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Start date': {'table': 'grant_info', 'field': 'start_date', 'optional': True, 'searchable': False, 'list': None}}, 
# {'End date': {'table': 'grant_info', 'field': 'end_date', 'optional': True, 'searchable': False, 'list': None}}, 
# {'Lead PI Contributor ID': {'table': 'contributor', 'field': 'id', 'optional': True, 'searchable': True, 'list': None}}

pull_project_rows = """
    SELECT
       project.short_name,
       project.title,
       project.description,
       program.name,
       project.url_knowledgebase,
       project.comment,
       project.project_type,
       project.is_grant,
       lab.lab_name,
       contributor.name,
       grant_info.grant_number,
       grant_info.funding_agency,
       grant_info.description_url,
       grant_info.start_date,
       grant_info.end_date,
       contributor.name
    FROM
       project
    INNER JOIN 
       program ON project.program_id = program.id
    INNER JOIN 
       grant_info ON project.id = grant_info.project_id
    INNER JOIN 
       project_has_contributor ON project.id = project_has_contributor.project_id
    INNER JOIN 
       contributor ON project_has_contributor.contrib_id = contributor.id
    INNER JOIN 
       lab ON contributor.lab_lab_id = lab.id;
"""
