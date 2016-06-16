#!/usr/bin/env python

import os
import csv
import sys
import logging as log
import json
import requests
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='import a ndar dataset to scitran')
    parser.add_argument('url',
                        help='scitran base url')
    parser.add_argument('user',
                        help='scitran base url')
    parser.add_argument('folderpath',
                        help='folder containing the ndar data')
    return parser.parse_args()

def generate_tree(folder, group):
    subjects_file = os.path.join(folder, 'ndar_aggregate.txt')
    images_file = os.path.join(folder, 'image03.txt')
    project_label = os.path.basename(folder)
    project = generate_project(project_label, group)
    sessions_dict = generate_sessions(subjects_file)
    acquisitions_dict = generate_acquisitions(images_file, sessions_dict)
    return project, sessions_dict, acquisitions_dict

def generate_project(project_label, group):
    return {
        'label': project_label,
        'group': group
    }

def generate_sessions(subjects_file):
    with open(subjects_file, 'rb') as f:
        sessions = {}
        rows = csv.DictReader(f, delimiter='\t')
        rows.next()
        for row in rows:
            sessions[row['subjectkey']] = scitran_session(row)
    return sessions

def scitran_session(ndar_subject):
    label = ndar_subject['subjectkey']
    subject = {
        'sex': ndar_subject['gender'].lower(),
        'age': _age_in_seconds(ndar_subject['interview_age']),
        'code': ndar_subject['subjectkey'],
        'metadata': ndar_subject
    }
    return {
        'label': label,
        'subject': subject
    }

def _age_in_seconds(age_in_months):
    if not age_in_months:
        return 0
    seconds_in_a_month = 2629800 # 24*3600*365.25/12
    return seconds_in_a_month * int(age_in_months)

def scitran_acquisition(ndar_image):
    label = ndar_image['image_file'].split('/')[-1].split('.')[0]
    return {
        'label': label,
        'metadata': ndar_image
    }

def generate_acquisitions(images_file, sessions):
    with open(images_file, 'rb') as f:
        acqs = {}
        rows = csv.DictReader(f, delimiter='\t')
        rows.next()
        for row in rows:
            acqs[row['subjectkey']] = acqs.get(row['subjectkey'], []) + [scitran_acquisition(row)]
    return acqs

def create(session, container_name, base_url, payload):
    data = json.dumps(payload)
    r = session.post(
        base_url + '/' + container_name,
        data=data
    )
    if r.status_code == 200:
        log.debug('{} has been created'.format(container_name[:-1]))
    else:
        raise Exception(r.reason)
    return json.loads(r.content)['_id']

def get_or_create_group(session, base_url, group):
    r = session.get(
        base_url + '/groups/' + group
    )
    if r.status_code == 200:
        log.debug('group already exists')
        return
    elif r.status_code == 404:
        data = json.dumps({
            '_id': group
        })
        r = session.post(
            base_url + '/groups',
            data=data
        )
        if r.status_code != 200:
            raise Exception(r.reason)
        else:
            log.debug('group has been created')
    else:
        raise Exception(r.reason)

def initialize_session(user):
    session = requests.Session()
    session.verify = True
    session.params = {
        'root': True,
        'user': user
    }
    return session

def main():
    args = parse_args()
    session = initialize_session(args.user)
    proj, sess_dict, acqs_dict = generate_tree(args.folderpath, 'ndar')
    get_or_create_group(session, args.url, 'ndar')
    proj_id = create(session, 'projects', args.url, proj)
    for subj, sess in sess_dict.iteritems():
        sess['project'] = proj_id
        if acqs_dict.get(subj):
            ses_id = create(session, 'sessions', args.url, sess)
            for acq in acqs_dict[subj]:
                acq['session'] = ses_id
                create(session, 'acquisitions', args.url, acq)
    print proj_id

if __name__=='__main__':
    main()