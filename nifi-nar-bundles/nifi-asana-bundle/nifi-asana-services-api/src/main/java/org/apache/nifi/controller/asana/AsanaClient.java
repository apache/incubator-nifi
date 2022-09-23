/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.asana;

import com.asana.models.Attachment;
import com.asana.models.Project;
import com.asana.models.ProjectMembership;
import com.asana.models.ProjectStatus;
import com.asana.models.Section;
import com.asana.models.Story;
import com.asana.models.Tag;
import com.asana.models.Task;
import com.asana.models.Team;
import com.asana.models.User;

import java.util.Map;

/**
 * This interface represents a client to Asana REST server, with some basic filtering options built in.
 */
public interface AsanaClient {
    /**
     * Find & retrieve an Asana project by its name. If multiple projects match, returns the first.
     * If there is no match, then {@link RuntimeException} is thrown. Note that constant ordering
     * is not guaranteed by Asana.
     *
     * @param projectName The name of the project you would like to retrieve. Case-sensitive.
     * @return An object representing the project.
     */
    Project getProjectByName(String projectName);

    /**
     * Retrieve all the projects from Asana without filtering them any further.
     *
     * @return {@link Map} of projects, where the key is the ID in Asana.
     */
    Map<String, Project> getProjects();

    /**
     * Retrieve all the users from Asana without filtering them any further.
     *
     * @return {@link Map} of users, where the key is the ID in Asana.
     */
    Map<String, User> getUsers();

    /**
     * Retrieve the users (members) assigned to a given project.
     *
     * @param project The object representing the project. Returned earlier by {@code getProjects()}
     *                or {@code getProjectByName()}.
     *
     * @return {@link Map} of project members, where the key is the ID of the project-user association in Asana.
     */
    Map<String, ProjectMembership> getProjectMemberships(Project project);

    /**
     * Find & retrieve an Asana team by its name. If multiple teams match, returns the first.
     * If there is no match, then {@link RuntimeException} is thrown. Note that constant ordering
     * is not guaranteed by Asana.
     *
     * @param teamName The name of the team you would like to retrieve. Case-sensitive.
     * @return An object representing the team.
     */
    Team getTeamByName(String teamName);

    /**
     * Retrieve all the teams from Asana without filtering them any further.
     *
     * @return {@link Map} of teams, where the key is the ID in Asana.
     */
    Map<String, Team> getTeams();

    /**
     * Retrieve the users (members) assigned to a given team.
     *
     * @param team The object representing the team. Returned earlier by {@code getTeams()}
     *             or {@code getTeamByName()}.
     *
     * @return {@link Map} of team members, where the key is the ID of the user in Asana.
     */
    Map<String, User> getTeamMembers(Team team);

    /**
     * Find & retrieve an Asana project's section (board column) by its name. If multiple sections
     * match, returns the first. If there is no match, then {@link RuntimeException} is thrown.
     * Note that constant ordering is not guaranteed by Asana.
     *
     * @param project The object representing the project. Returned earlier by {@code getProjects()}
     *                or {@code getProjectByName()}.
     * @param sectionName The name of the section (board column) you would like to retrieve. Case-sensitive.
     * @return An object representing the section.
     */
    Section getSectionByName(Project project, String sectionName);

    /**
     * Retrieve all the sections (board columns) of an Asana project without filtering them any further.
     *
     * @param project The object representing the project. Returned earlier by {@code getProjects()}
     *                or {@code getProjectByName()}.
     * @return {@link Map} of project sections, where the key is the ID of the section in Asana.
     */
    Map<String, Section> getSections(Project project);

    /**
     * Retrieve all the tasks from an Asana project without filtering them any further.
     *
     * @param project The object representing the project. Returned earlier by {@code getProjects()}
     *                or {@code getProjectByName()}.
     * @return {@link Map} of project tasks, where the key is the ID of the task in Asana.
     */
    Map<String, Task> getTasks(Project project);

    /**
     * Retrieve all the tasks tagged with a given tag.
     *
     * @param tag The object representing the tag. Returned earlier by {@code getTags()}.
     * @return {@link Map} of tasks, where the key is the ID in Asana.
     */
    Map<String, Task> getTasks(Tag tag);

    /**
     * Retrieve all the tasks from a given section.
     *
     * @param section The object representing the section. Returned earlier by {@code getSections()}
     *                or {@code getSectionByName()}.
     * @return {@link Map} of tasks, where the key is the ID in Asana.
     */
    Map<String, Task> getTasks(Section section);

    /**
     * Retrieve all the tags from Asana without filtering them any further.
     *
     * @return {@link Map} of tags, where the key is the ID in Asana.
     */
    Map<String, Tag> getTags();

    /**
     * Retrieve all the status updates of an Asana project without filtering them any further.
     *
     * @param project The object representing the project. Returned earlier by {@code getProjects()}
     *                or {@code getProjectByName()}.
     * @return {@link Map} of project status updates, where the key is the ID in Asana.
     */
    Map<String, ProjectStatus> getProjectStatusUpdates(Project project);

    /**
     * Retrieve all the stories (comments) of an Asana task without filtering them any further.
     *
     * @param task The object representing the task. Returned earlier by {@code getTasks()}.
     * @return {@link Map} of stories (comments), where the key is the ID in Asana.
     */
    Map<String, Story> getStories(Task task);

    /**
     * Retrieve all the attachments of an Asana task without filtering them any further.
     *
     * @param task The object representing the task. Returned earlier by {@code getTasks()}.
     * @return {@link Map} of attachments, where the key is the ID in Asana.
     */
    Map<String, Attachment> getAttachments(Task task);

    /**
     * Retrieve all the attachments of an Asana project status update without filtering them any further.
     *
     * @param projectStatus The object representing the project's status. Returned earlier by
     *                      {@code getProjectStatusUpdates()}.
     * @return {@link Map} of attachments, where the key is the ID in Asana.
     */
    Map<String, Attachment> getAttachments(ProjectStatus projectStatus);

    /**
     * Subscribes the event stream of a project, and incrementally returns new events created since the last call.
     *
     * @param project The object representing the project. Returned earlier by {@code getProjects()}
     *                or {@code getProjectByName()}.
     * @param syncToken Token returned by the previous call. If the token is empty, null, or invalid, a new token will be generated,
     *                  but events since the last call won't be fetched.
     * @return An {@link AsanaEventsCollection} containing a token and the collection of events created since the last call.
     */
    AsanaEventsCollection getEvents(Project project, String syncToken);
}
