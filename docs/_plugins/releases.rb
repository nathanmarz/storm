module Releases
  class Generator < Jekyll::Generator
    def dir_to_releasename(dir)
      ret = nil
      splitdir = dir.split("/").select{ |a| a != ""};
      if (splitdir[0] == 'releases')
        ret = splitdir[1]
        if (ret == 'current')
          ret = File.readlink(splitdir.join("/")).split("/")[-1]
        end
      end
      return ret
    end

    def set_if_unset(hash, key, value)
      hash[key] = hash[key] || value;
    end

    def parse_version(version_string)
      return version_string.split('.').map{|e| e.to_i}
    end

    def release_from_pom()
      text= `mvn -f ../pom.xml help:evaluate -Dexpression=project.version`
      return text.split("\n").select{|a| !a.start_with?('[')}[0]
    end

    def branch_from_git()
      return `git rev-parse --abbrev-ref HEAD`
    end

    def generate(site)
      if site.config['storm_release_only']
        release_name = release_from_pom()
        puts "release: #{release_name}"
        git_branch = branch_from_git()
        puts "branch: #{git_branch}"
        for page in site.pages do
          page.data['version'] = release_name;
          page.data['git-tree-base'] = "http://github.com/apache/storm/tree/#{git_branch}"
          page.data['git-blob-base'] = "http://github.com/apache/storm/blob/#{git_branch}"
        end
        return
      end

      releases = Hash.new
      if (site.data['releases'])
        for rel_data in site.data['releases'] do
          releases[rel_data['name']] = rel_data
        end
      end

      for page in site.pages do
        release_name = dir_to_releasename(page.dir)
        if (release_name != nil)
          if !releases.has_key?(release_name)
            releases[release_name] = {'name' => release_name};
          end
          releases[release_name]['documented'] = true
        end
      end

      releases.each { |release_name, release_data|
          set_if_unset(release_data, 'git-tag-or-branch', "v#{release_data['name']}")
          set_if_unset(release_data, 'git-tree-base', "http://github.com/apache/storm/tree/#{release_data['git-tag-or-branch']}")
          set_if_unset(release_data, 'git-blob-base', "http://github.com/apache/storm/blob/#{release_data['git-tag-or-branch']}")
          set_if_unset(release_data, 'base-name', "apache-storm-#{release_data['name']}")
          set_if_unset(release_data, 'has-download', !release_name.end_with?('-SNAPSHOT'))
      }

      for page in site.pages do
        release_name = dir_to_releasename(page.dir)
        if (release_name != nil)
          release_data = releases[release_name] 
          page.data['version'] = release_name;
          page.data['git-tree-base'] = release_data['git-tree-base'];
          page.data['git-blob-base'] = release_data['git-blob-base'];
        end
      end
      site.data['releases'] = releases.values.sort{|x,y| parse_version(y['name']) <=>
                                                         parse_version(x['name'])};
    end
  end
end
