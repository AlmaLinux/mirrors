module.exports = {
  description: 'List of AlmaLinux official mirrors',
  head: [
    ['link', { rel: "shortcut icon", type: 'image/png', href: "/images/logo.png"}],
  ],
  base: '/',
  themeConfig: {
    logo: '/images/logo.png',
    nav: [
      { text: 'Home', link: 'https://almalinux.org/' },
      { text: 'Blog', link: 'https://blog.almalinux.org/' },
      { text: 'Bugs', link: 'https://bugs.almalinux.org/' }
    ],
    // AlmaLinux organization on GitHub
    repo: 'AlmaLinux/',
    // mirrors repository settings
    docsRepo: 'AlmaLinux/mirrors',
    docsDir: 'docs',
    docsBranch: 'master',
    editLinks: false,
    search: false
  }
}
