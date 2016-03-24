%% Check that you can get a token
%
% LMP/BW Scitran Team, 2016

%% Check that the database is up

% It is possible to create, refresh, or revoke
clientID = 'scitran';
action   = 'create';  
[token, result] = sdmAuth(action,clientID);

% If result returns 0, then we are good
if result==0
    disp('A token was created')
else
    disp(result)
end

% The token is a long, obscure string
disp(token)

%% If you have lost the token by a matlab clear you might run

clientID = 'scitran';
action   = 'refresh';  
[token, result] = sdmAuth(action,clientID);

% If result returns 0, then we are good
if result==0
    disp('A token was refreshed')
else
    disp(result)
end


%% To revoke, you will actually remove the secret and client ID

% clientID = 'scitran';
% action   = 'revoke';  
% [token, result] = sdmAuth(action,clientID);

% token will be empty and result 0 if successful.

%%