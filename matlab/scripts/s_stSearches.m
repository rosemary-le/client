%% Scitran search
%
% It is possible to interact with the database, either to process the files
% or to discover content, using Matlab or Python.  This script illustrates
% various ways to search the database to find files and experiments.
%
% The basic objects in the data base comprise individual files,
% acquisitions (groups of related files), sessions, and projects. These
% objects are described a little more below and also on the scitran/client
% wiki page.
%
% The scitran client supports search the data base to identify these
% objects. The principle of the search is simple:
%
%    * Authorization to access the scitran site (st = scitran)
%    * Create a Matlab structure (srch) to specify search requirements
%    * Run Search (results = st.search(srch))
%    * Results are a cell array of objects that meet the search criterion
% 
% For further and developing documentation, see
%
%  * <https://github.com/scitran/client/wiki scitran/client wiki page>, and
%  specifically the pages on 
%
%  * <https://github.com/scitran/client/wiki/Search Search> and the
%  * <https://github.com/scitran/client/wiki/Search-examples search examples>
%  
% Searching for an object
%
%  Searches begin by defining the type of object you are looking for (e.g.,
%  files).  Then we define the required features of the object.
%
%  We set the terms of the search by  creating a Matlab struct.  The first
%  slot in the struct defines the type of object you are searching for.
%  Suppose we call the struct 'srch'.  The srch.path slot defines the kind
%  of object we are searching for.
%
%   srch.path = 'projects'
%   srch.path = 'sessions'    
%   srch.path = 'acquisitions'
%   srch.path = 'files'        
%   srch.path = 'analysis'
%   srch.path = 'collections'
%
% The search operations are specified by adding additional slots to the
% struct, 'srch'.  These includes specific operators, parameters, and
% values.  The point of this script is to provide many examples of how to
% set up these searches
% 
% Important operators that we use below are
%
%   'match', 'bool', 'must', 'range'
%
% Important parameters we use in search are 
%   'name', 'group', 'label', 'id','plink', subject_0x2E_age',
%   'container_id', 'type'.  
%
% A list of searchable terms can be found in the scitran/core
%
%  <https://github.com/scitran/core/wiki/Data-Model Data Model page>.
%
% Other operators are possible (e.g. 'filtered','filter','query','not') but
% here we illustrate the basics. 
%
% See also:  st.browser - we use this function to visualize the returned
%            object in the browser.
%
% LMP/BW Scitran Team, 2016

%% Authorization

% The auth returns a token and the url of the flywheel instance.  These are
% fixed as part of 's' throughout the examples, below.
st = scitran('action', 'create', 'instance', 'scitran');

%% List all projects

% In this example, we use the structure 'srch' to store the search parameters.
% When we are satisfied with the parameters, we attach srch to the mean search
% structure, s, and then run the search command.

clear srch
srch.path = 'projects';
projects = st.search(srch);
fprintf('Found %d projects\n',length(projects))

%% List projects in the group 'wandell' with label 'vwfa'

clear srch
srch.path = 'projects';
srch.projects.bool.must(1).match.group = 'wandell';
srch.projects.bool.must(2).match.label = 'vwfa';

% srch.projects.match.label = 'vwfa'
projects = st.search(srch);
fprintf('Found %d projects for the group wandell, with label vwfa.\n',length(projects))

% Save this project information
projectID    = projects{1}.id;
projectLabel = projects{1}.source.label;

% You can browse to the project this way
%   st.browser(projects{1});

%% Get all the sessions within a specific collection
clear srch; 
srch.path = 'sessions';
srch.collections.match.label = 'GearTest';
sessions = st.search(srch);

fprintf('Found %d sessions in GearTest\n',length(sessions));

%% Get the sessions within the first project

clear srch
srch.path = 'sessions';
srch.projects.match.x0x5F_id = projectID;   % Note the ugly x0x5F.  See notes.
sessions = st.search(srch);
fprintf('Found %d sessions in the project %s\n',length(sessions),projectLabel);

% Save this session information
sessionID = sessions{1}.id;
sessionLabel = sessions{1}.source.label;

% Bring the session up this way
%   st.browser(sessions{1});

%% Get the acquisitions inside a session

clear srch
srch.path = 'acquisitions';
srch.sessions.match.x0x5F_id = sessionID;
s.json = srch;
acquisitions = st.search(srch);
fprintf('Found %d acquisitions in the session %s\n',length(acquisitions),sessionLabel);

% This brings up the session containing this acquisition
%   st.browser(acquisitions{1});

%% Find nifti files in the session

clear srch
srch.path = 'files';
srch.sessions.match.x0x5F_id = sessionID;
srch.files.match.type = 'nifti';
files = st.search(srch);

nFiles = length(files);
fprintf('Found %d nifti files in the session %s\n',nFiles,sessionLabel);
for ii=1:nFiles
    fprintf('%d:  %s\n',ii,files{ii}.source.name);
end

% Download a file this way
%
%  dl = stGet(files{1}.plink,s.token)
%  d = niftiRead(dl);

%% Look for analyses in the GearTest collection

clear srch
srch.path = 'analyses';
srch.collections.match.label = 'GearTest';
analyses = st.search(srch);

% Which collection is the analysis in?
clear srch; 
srch.path = 'collections';
srch.collections.match.label = 'GearTest';
s.json = srch;
collections = st.search(srch);

% Find a session from that collection
clear srch; 
srch.path = 'sessions'; 
srch.collections.match.label = 'GearTest';
sessions = st.search(srch);

% Bring up the browser to that collection and session
url = st.browser(sessions{1},'collection',collections{1});

%% Count the number of sessions created in a recent time period

clear srch
srch.path = 'collections';
srch.sessions.range.created.gte = 'now-4w';  % For weeks ago
collections = st.search(srch);
fprintf('Found %d collections in previous four weeks \n',length(collections))

% To see the sessions within some time range use:
%   srch.sessions.range.created.gte = dateFormat;
%   srch.sessions.range.created.lte = dateFormat;
%
% To see the session in the web page, use this command
%   st.browser(sessions{1});


%% Get sessions with this subject code
clear srch
srch.path = 'sessions';
subjectCode = 'ex4842';
srch.sessions.match.subject_0x2E_code = subjectCode;
sessions = st.search(srch);
fprintf('Found %d sessions with subject code %s\n',length(sessions),subjectCode)

% Click to 'Subject' tab to see the subject code
%    st.browser(s.url,sessions{1});

%% Get sessions in which the subject age is within a range

clear srch
srch.path = 'sessions';
srch.sessions.bool.must{1}.range.subject_0x2E_age.gt = year2sec(10);
srch.sessions.bool.must{1}.range.subject_0x2E_age.lt = year2sec(15);
sessions = st.search(srch);

fprintf('Found %d sessions\n',length(sessions))

% View it in the browser and click on the subject tab to see the age
%   st.browser(sessions{1})

%% Find a session with a specific label

sessionLabel = '20151128_1621';

clear srch
srch.path = 'sessions';
srch.sessions.match.label= sessionLabel;
sessions = st.search(srch);
fprintf('Found %d sessions with the label %s\n',length(sessions),sessionLabel)

% Browse the session.
%   st.browser(sessions{1});

%% get files from a particular project, session, and acquisition

clear srch
srch.path = 'files';
srch.projects.match.label = 'testproject';
srch.sessions.match.label = '20120522_1043';
srch.acquisitions.match.label = '11_1_spiral_high_res_fieldmap';
srch.files.match.type = 'nifti';
files = st.search(srch);
fprintf('Found %d matches to this files label\n',length(files));

% This is how to download the nifti file
%  fname = files{1}.source.name;
%  dl_file = stGet(files{1}, s.token, 'destination', fullfile(pwd,fname));
%  d = niftiRead(dl_file);


%%
clear srch
srch.path = 'files';   
srch.collections.match.label = 'Young Males';
srch.acquisitions.match.label = 'SPGR 1mm 30deg';
srch.files.match.type = 'nifti';

files = st.search(srch);

%%
clear srch
srch.path = 'files';   
srch.collections.match.label = 'Young Males';
srch.acquisitions.match.measurement = 'Diffusion';
srch.files.match.type = 'bvec';

files = st.search(srch);

%% get files in project/session/acquisition/collection

clear srch
srch.path = 'files';                         % Looking for T1 weighted files
srch.collections.match.label = 'ENGAGE';     % Collection is ENGAGE
srch.acquisitions.match.label = 'T1w 1mm';   % Description column
files = st.search(srch);
fprintf('Found %d matching files\n',length(files))

%% get files from a collection

clear srch
srch.path = 'files'; 
srch.collections.match.label = 'Young Males';   
srch.acquisitions.match.label = 'SPGR 1mm 30deg';   
srch.files.match.name = '16.1_dicom_nifti.nii.gz';

files = st.search(srch);
fprintf('Found %d matching files\n',length(files))

for ii=1:length(files)
    files{ii}.source
end

%% Find Public Data
%

clear srch
srch.path = 'projects';
srch.projects.match.exact_label = 'Public Data';
savejson('',srch)
projects = st.search(srch);





%% Matlab structs and JSON format
%
%  Matlab uses '.' in structs, and json allows '.' as part of the
%  variable name. So, we insert a dot on the Matlab side by inserting a
%  string, _0x2E_.  For example, to create a json object like this:
%
%   s = {
%   	"range": {
%   		"subject.age": {
% 	   		  "lte": years2sec(10)
% 		    }
% 	       }
%       }
%
% We use the code
%     clear srch; 
%     srch.range.subject_0x2E_age.lte = years2sec(10);
%
% Another issue is the use of _ at the front of a variable, like _id
%
% In this case, we cannot set srch._id in Matlab.  But we can set
%    srch.projects.match.x0x5F_id = projectID;
%
% The savejson('',srch) returns the variable as simply _id, without all the
% x0x5F nonsense.
% 
%% DRAFT:  Do not use for now.
%
% There are various special cases when you set the path, as well.  Note
% that some of these have a single '/' and some have a double '//'.
%
%   srch.path = 'acquisitions/files'  files within a specific acquisition
%   srch.path = 'sessions//files'     files contained in sessions and
%            acquisitions (excluding files attached to a collection or a
%            project). The double slash ("//") means take every descendant
%            of the matched sessions that is a file. 
%   srch.path = 'sessions//' "//" means every descendant of the matched
%            sessions, including the sessions themselves. That is
%            sessions, files, notes, analyses in these sessions,
%            acquisitions belonging to these sessions, files, notes,
%            analyses in these acquisitions.   
%   srch.path = 'collections//files'   Files in the matched collections
%   srch.path = 'collections//acquisitions' all acquisitions that belongs to
%            the matched collections. 
%
% See also:
%   