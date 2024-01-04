clc; close all;
format longG;
data = load("skins.txt"); %name of file here
data = sort(data);
k = 500;
disp(['kth element is ', num2str(data(k))]);
